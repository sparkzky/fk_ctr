// // use containerd_client::services::v1::images_client::ImagesClient;

// // async fn prepare_image(image_name: &str) {

// //     let arch =
// //         "amd64";

// //     let channel = client::connect("/run/containerd/containerd.sock")
// //         .await
// //         .expect("Connect Failed");
// //     let mut client = TransferClient::new(channel.clone());

// //     // Create the source (OCIRegistry)
// //     let source = OciRegistry {
// //         reference: image_name.to_string(),
// //         resolver: Default::default(),
// //     };

// //     let platform = Platform {
// //         os: "linux".to_string(),
// //         architecture: arch.to_string(),
// //         variant: "".to_string(),
// //         os_version: "".to_string(),
// //     };

// //     // Create the destination (ImageStore)
// //     let destination = ImageStore {
// //         name: image_name.to_string(),
// //         platforms: vec![platform.clone()],
// //         unpacks: vec![UnpackConfiguration {
// //             platform: Some(platform),
// //             ..Default::default()
// //         }],
// //         ..Default::default()
// //     };

// //     let anys = to_any(&source);
// //     let anyd = to_any(&destination);

// //     println!("Pulling image for linux/{} from source: {:?}", arch, source);

// //     // Create the transfer request
// //     let request = TransferRequest {
// //         source: Some(anys),
// //         destination: Some(anyd),
// //         options: Some(TransferOptions {
// //             ..Default::default()
// //         }),
// //     };
// //     // Execute the transfer (pull)
// //     client
// //         .transfer(with_namespace!(request, NAMESPACE))
// //         .await
// //         .expect("unable to transfer image");

// //     let client=ImagesClient::new(channel.clone());

// //     // 触发解压（参考搜索结果的镜像解压逻辑）
// //     let image = client.get(Request::new(GetImageRequest {
// //         name: image_name.to_string(),
// //     })).await.expect("Image not found").into_inner();

// //     if !image.unpacked {
// //         client.unpack(Request::new(UnpackImageRequest {
// //             image: image_name.to_string(),
// //             snapshotter: "overlayfs".to_string(),
// //         })).await.expect("Failed to unpack image");
// //     }
// // }

mod cni;
mod lib_;
use client::{
    services::v1::{
        container::Runtime, containers_client::ContainersClient, tasks_client::TasksClient,
        version_client::VersionClient, Container, CreateContainerRequest, CreateTaskRequest,
        DeleteContainerRequest, DeleteTaskRequest, StartRequest, WaitRequest,
    },
    with_namespace,
};
use cni::{create_cni_network, delete_cni_network, init_net_work};
use containerd_client::{
    self as client,
    services::v1::{
        content_client::ContentClient, images_client::ImagesClient,
        namespaces_client::NamespacesClient, GetImageRequest, Image, ListNamespacesRequest,
        ReadContentRequest,
    },
};
use containerd_client::{
    services::v1::{
        snapshots::{snapshots_client::SnapshotsClient, PrepareSnapshotRequest},
        transfer_client::TransferClient,
        TransferOptions, TransferRequest,
    },
    to_any,
    types::{
        transfer::{ImageStore, OciRegistry, UnpackConfiguration},
        Platform,
    },
};
use oci_spec::image::{Arch, ImageConfiguration, ImageIndex, ImageManifest, MediaType, Os};
use prost_types::Any;
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    thread::sleep,
    time::Duration,
};
use tonic::{transport::Channel, Request};

const CID: &str = "test_cni_031";
const NAMESPACE: &str = "default";
const IMAGE: &str = "docker.io/library/nginx:alpine";

/// Make sure you run containerd before running this example.
/// NOTE: to run this example, you must prepare a rootfs.
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let channel = client::connect("/run/containerd/containerd.sock")
        .await
        .expect("Connect Failed");
    let arch = "amd64";
    let image = IMAGE;
    let namespace = "default";
    let mut client = TransferClient::new(channel.clone());

    // Create the source (OCIRegistry)
    let source = OciRegistry {
        reference: image.to_string(),
        resolver: Default::default(),
    };

    let platform = Platform {
        os: "linux".to_string(),
        architecture: arch.to_string(),
        variant: "".to_string(),
        os_version: "".to_string(),
    };

    // Create the destination (ImageStore)
    let destination = ImageStore {
        name: image.to_string(),
        platforms: vec![platform.clone()],
        unpacks: vec![UnpackConfiguration {
            platform: Some(platform),
            ..Default::default()
        }],
        ..Default::default()
    };

    let anys = to_any(&source);
    let anyd = to_any(&destination);

    println!("Pulling image for linux/{} from source: {:?}", arch, source);

    // Create the transfer request
    let request = TransferRequest {
        source: Some(anys),
        destination: Some(anyd),
        options: Some(TransferOptions {
            ..Default::default()
        }),
    };
    // Execute the transfer (pull)
    client
        .transfer(with_namespace!(request, namespace))
        .await
        .expect("unable to transfer image");
    // let imgconf = get_img_config(IMAGE.to_string(), &channel).await.unwrap();
    // let mut env = vec![];
    // let mut args = vec![];
    // if let Some(config) = imgconf.config() {
    //     env = config.env().as_ref().map_or_else(Vec::new, |v| v.clone());
    //     args = config.cmd().as_ref().map_or_else(Vec::new, |v| v.clone());
    // };
    // let mut spec = populate_default_unix_spec(CID, NAMESPACE);
    // spec.process.args = args;
    // spec.process.env = env;
    // save_spec_to_file(
    //     &spec,
    //     "/home/dragonos/for_faas/fucking-test/target/test.json",
    // )
    // .unwrap();
    init_net_work().unwrap();

    let (ip, path) = create_cni_network(CID.to_string(), NAMESPACE.to_string()).unwrap();
    println!("IP: {:?}, Path: {:?}", ip, path);

    let parent_snapshot = parent_snapshot(IMAGE.to_string(), &channel).await.unwrap();
    println!("{:?}", parent_snapshot);
    let req = PrepareSnapshotRequest {
        snapshotter: "overlayfs".to_string(),
        key: CID.to_string(),
        parent: parent_snapshot,
        ..Default::default()
    };
    let mut c = SnapshotsClient::new(channel.clone());
    let resp_pre = c
        .prepare(with_namespace!(req, NAMESPACE))
        .await
        .expect("Failed to prepare snapshot")
        .into_inner();
    println!("mounts: {:?}", resp_pre.mounts);
    println!("key: {}", CID.to_string());
    println!("{:?}", resp_pre);

    let mut client = ContainersClient::new(channel.clone());

    // let rootfs = "/home/dragonos/for_faas/fucking-test/rootfs";
    // let only_rootfs = "rootfs";
    // the container will run with command `echo $output`

    let spec = include_str!("/home/dragonos/for_faas/fucking-test/src/111.json");
    let spec = spec.to_string();
    // .replace("$ROOTFS", only_rootfs)
    // .replace("$NAME", CID);

    let spec = Any {
        type_url: "types.containerd.io/opencontainers/runtime-spec/1/Spec".to_string(),
        value: spec.into_bytes(),
    };

    let container = Container {
        id: CID.to_string(),
        image: IMAGE.to_string(),
        runtime: Some(Runtime {
            name: "io.containerd.runc.v2".to_string(),
            options: None,
        }),
        spec: Some(spec),
        snapshotter: "overlayfs".to_string(),
        snapshot_key: CID.to_string(),
        ..Default::default()
    };

    let req = CreateContainerRequest {
        container: Some(container),
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client
        .create(req)
        .await
        .expect("Failed to create container");

    println!("Container: {:?} created", CID);

    // create temp dir for stdin/stdout/stderr
    let tmp = std::env::temp_dir().join("containerd-client-test");
    fs::create_dir_all(&tmp).expect("Failed to create temp directory");
    let stdin = tmp.join("stdin");
    let stdout: std::path::PathBuf = tmp.join("stdout");
    let stderr = tmp.join("stderr");
    File::create(&stdin).expect("Failed to create stdin");
    File::create(&stdout).expect("Failed to create stdout");
    File::create(&stderr).expect("Failed to create stderr");

    // creat and start task
    let mut client = TasksClient::new(channel.clone());

    let req = CreateTaskRequest {
        container_id: CID.to_string(),
        stdin: stdin.to_str().unwrap().to_string(),
        stdout: stdout.to_str().unwrap().to_string(),
        stderr: stderr.to_str().unwrap().to_string(),
        rootfs: resp_pre.mounts,
        ..Default::default()
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client.create(req).await.expect("Failed to create task");

    println!("Task: {:?} created", CID);

    println!("Sleeping for 30 seconds");
    sleep(Duration::from_secs(30));

    let req = StartRequest {
        container_id: CID.to_string(),
        ..Default::default()
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client.start(req).await.expect("Failed to start task");

    println!("Task: {:?} started", CID);

    // wait task
    let req = WaitRequest {
        container_id: CID.to_string(),
        ..Default::default()
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client.wait(req).await.expect("Failed to wait task");

    println!("Task: {:?} stopped", CID);

    // delete task
    let req = DeleteTaskRequest {
        container_id: CID.to_string(),
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client.delete(req).await.expect("Failed to delete task");

    println!("Task: {:?} deleted", CID);

    // delete container
    let mut client = ContainersClient::new(channel);

    let req = DeleteContainerRequest {
        id: CID.to_string(),
    };
    let req = with_namespace!(req, NAMESPACE);

    let _resp = client
        .delete(req)
        .await
        .expect("Failed to delete container");

    println!("Container: {:?} deleted", CID);

    delete_cni_network(NAMESPACE, CID);

    // test container output
    let actual_stdout = fs::read_to_string(stdout).expect("read stdout actual");
    println!("stdout: {:?}", actual_stdout);
    // assert_eq!(actual_stdout.strip_suffix('\n').unwrap(), output);

    // clear stdin/stdout/stderr
    let _ = fs::remove_dir_all(tmp);

    //? 111hfhfh
}

async fn get_img_config(name: String, channel: &Channel) -> Option<ImageConfiguration> {
    let mut c = ImagesClient::new(channel.clone());

    let req = GetImageRequest { name };
    let resp = c
        .get(with_namespace!(req, "default"))
        .await
        .expect("NONONO")
        .into_inner();
    let img_dscr = resp.image.unwrap().target.unwrap();
    let media_type = MediaType::from(img_dscr.media_type.as_str());

    let req = ReadContentRequest {
        digest: img_dscr.digest,
        ..Default::default()
    };
    let mut c = ContentClient::new(channel.clone());

    let resp = c
        .read(with_namespace!(req, "default"))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap()
        .data;
    let img_config = match media_type {
        MediaType::ImageIndex => handle_index(&resp, channel).await.unwrap(),
        MediaType::ImageManifest => handle_manifest(&resp, channel).await.unwrap(),
        MediaType::Other(media_type) => match media_type.as_str() {
            "application/vnd.docker.distribution.manifest.list.v2+json" => {
                handle_index(&resp, channel).await.unwrap()
            }
            "application/vnd.docker.distribution.manifest.v2+json" => {
                handle_manifest(&resp, channel).await.unwrap()
            }
            _ => panic!("unexpected media type '{media_type}' ({media_type:?})"),
        },
        _ => panic!("unexpected media type '{media_type}' ({media_type:?})"),
    };
    Some(img_config)
}

async fn parent_snapshot(name: String, channel: &Channel) -> Option<String> {
    let img_config = get_img_config(name, channel).await.unwrap();

    let mut iter = img_config.rootfs().diff_ids().iter();
    let mut ret = iter
        .next()
        .map_or_else(String::new, |layer_digest| layer_digest.clone());

    while let Some(layer_digest) = iter.next() {
        let mut hasher = Sha256::new();
        hasher.update(ret.as_bytes());
        ret.push_str(&format!(",{}", layer_digest));
        hasher.update(" ");
        hasher.update(layer_digest);
        let digest = ::hex::encode(hasher.finalize());
        ret = format!("sha256:{digest}");
    }
    Some(ret)
}

async fn handle_index(data: &Vec<u8>, channel: &Channel) -> Option<ImageConfiguration> {
    let image_index: ImageIndex = ::serde_json::from_slice(&data).unwrap();
    let img_manifest_dscr = image_index
        .manifests()
        .iter()
        .find(|manifest_entry| match manifest_entry.platform() {
            Some(p) => {
                #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
                {
                    matches!(p.architecture(), &Arch::Amd64) && matches!(p.os(), &Os::Linux)
                }
                #[cfg(target_arch = "aarch64")]
                {
                    matches!(p.architecture(), &Arch::ARM64) && matches!(p.os(), Os::Linux)
                    //&& matches!(p.variant().as_ref().map(|s| s.as_str()), Some("v8"))
                }
            }
            None => false,
        })
        .unwrap();
    let req = ReadContentRequest {
        digest: img_manifest_dscr.digest().to_owned(),
        offset: 0,
        size: 0,
    };
    let mut c = ContentClient::new(channel.clone());
    let resp = c
        .read(with_namespace!(req, NAMESPACE))
        .await
        .expect("failed to read content")
        .into_inner()
        .message()
        .await
        .expect("failed to read content message")
        .unwrap()
        .data;

    handle_manifest(&resp, channel).await
}

async fn handle_manifest(data: &Vec<u8>, channel: &Channel) -> Option<ImageConfiguration> {
    let img_manifest: ImageManifest = ::serde_json::from_slice(&data).unwrap();
    let img_manifest_dscr = img_manifest.config();
    let req = ReadContentRequest {
        digest: img_manifest_dscr.digest().to_owned(),
        offset: 0,
        size: 0,
    };
    let mut c = ContentClient::new(channel.clone());

    let resp = c
        .read(with_namespace!(req, NAMESPACE))
        .await
        .unwrap()
        .into_inner()
        .message()
        .await
        .unwrap()
        .unwrap()
        .data;

    ::serde_json::from_slice(&resp).unwrap()
}

// async fn get_image(name: String, channel: &Channel) -> Option<Image> {
//     let mut c = ImagesClient::new(channel.clone());
//     let req = GetImageRequest { name };
//     c.get(with_namespace!(req, "default"))
//         .await
//         .unwrap()
//         .into_inner()
//         .image
// }

// pub async fn version(channel: &Channel) -> Option<(String, String)> {
//     let mut c = VersionClient::new(channel.clone());
//     let resp = c
//         .version(())
//         .await
//         .expect("Failed to get version")
//         .into_inner();
//     Some((resp.version, resp.revision))
// }

// #[tokio::main(flavor = "current_thread")]
// async fn main() {
//     let arch = "amd64";
//     let image = "docker.io/library/redis:alpine";
//     let namespace = "default";
//     let channel = client::connect("/run/containerd/containerd.sock")
//         .await
//         .expect("Connect Failed");
//     let mut client = TransferClient::new(channel.clone());

//     // Create the source (OCIRegistry)
//     let source = OciRegistry {
//         reference: image.to_string(),
//         resolver: Default::default(),
//     };

//     let platform = Platform {
//         os: "linux".to_string(),
//         architecture: arch.to_string(),
//         variant: "".to_string(),
//         os_version: "".to_string(),
//     };

//     // Create the destination (ImageStore)
//     let destination = ImageStore {
//         name: image.to_string(),
//         platforms: vec![platform.clone()],
//         unpacks: vec![UnpackConfiguration {
//             platform: Some(platform),
//             ..Default::default()
//         }],
//         ..Default::default()
//     };

//     let anys = to_any(&source);
//     let anyd = to_any(&destination);

//     println!("Pulling image for linux/{} from source: {:?}", arch, source);

//     // Create the transfer request
//     let request = TransferRequest {
//         source: Some(anys),
//         destination: Some(anyd),
//         options: Some(TransferOptions {
//             ..Default::default()
//         }),
//     };
//     // Execute the transfer (pull)
//     client
//         .transfer(with_namespace!(request, namespace))
//         .await
//         .expect("unable to transfer image");
// }

// #[tokio::main(flavor = "current_thread")]
// async fn main() {
//     let channel = client::connect("/run/containerd/containerd.sock")
//         .await
//         .expect("Connect Failed");
//     let mut c = NamespacesClient::new(channel.clone());
//     let req = ListNamespacesRequest {
//         ..Default::default()
//     };
//     let resp = c
//         .list(req)
//         .await
//         .unwrap()
//         .into_inner()
//         .namespaces
//         .into_iter()
//         .map(|ns| ns.name)
//         .collect::<Vec<_>>();
//     println!("{:?}", resp);

//     // println!("{:?}", resp);
// }

// let rootfs = "/home/dragonos

// 创建任务的时候才有cni
