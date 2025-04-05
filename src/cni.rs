type Err = Box<dyn std::error::Error>;
use serde_json::Value;
use std::{
    fmt::Error,
    fs::{self, File},
    io::{self, BufRead, Write},
    net::IpAddr,
    path::Path,
};
const CNI_BIN_DIR: &str = "/opt/cni/bin";
const CNI_CONF_DIR: &str = "/etc/cni/net.d";
const NET_NS_PATH_FMT: &str = "/proc/{}/ns/net";
const CNI_DATA_DIR: &str = "/var/run/cni";
const DEFAULT_CNI_CONF_FILENAME: &str = "10-faasrs.conflist";
const DEFAULT_NETWORK_NAME: &str = "faasrs-cni-bridge";
const DEFAULT_BRIDGE_NAME: &str = "faasrs0";
const DEFAULT_SUBNET: &str = "10.66.0.0/16";
const DEFAULT_IF_PREFIX: &str = "eth";

fn default_cni_conf() -> String {
    format!(
        r#"
{{
    "cniVersion": "0.4.0",
    "name": "{}",
    "plugins": [
      {{
        "type": "bridge",
        "bridge": "{}",
        "isGateway": true,
        "ipMasq": true,
        "ipam": {{
            "type": "host-local",
            "subnet": "{}",
            "dataDir": "{}",
            "routes": [
                {{ "dst": "0.0.0.0/0" }}
            ]
        }}
      }},
      {{
        "type": "firewall"
      }}
    ]
}}
"#,
        DEFAULT_NETWORK_NAME, DEFAULT_BRIDGE_NAME, DEFAULT_SUBNET, CNI_DATA_DIR
    )
}

pub fn init_net_work() -> Result<(), Err> {
    if !dir_exists(Path::new(CNI_CONF_DIR)) {
        fs::create_dir_all(CNI_CONF_DIR)?;
    }
    let net_config = Path::new(CNI_CONF_DIR).join(DEFAULT_CNI_CONF_FILENAME);
    let mut file = File::create(&net_config)?;
    file.write_all(default_cni_conf().as_bytes())?;

    Ok(())
}

fn get_netns(ns: &str, cid: &str) -> String {
    format!("{}-{}", ns, cid)
}
fn get_path(netns: &str) -> String {
    format!("/var/run/netns/{}", netns)
}

pub fn create_cni_network(cid: String, ns: String) -> Result<(String, String), Err> {
    // let netid = format!("{}-{}", cid, pid);
    let netns = get_netns(ns.as_str(), cid.as_str());
    let path = get_path(netns.as_str());
    let mut ip = String::new();

    let output = std::process::Command::new("ip")
        .arg("netns")
        .arg("add")
        .arg(&netns)
        .output()?;
    println!("ip netns add: {:?}", output);

    if !output.status.success() {
        return Err(Box::new(Error));
    }

    let add_command = format!(
        "export CNI_PATH=/opt/cni/bin && cnitool add faasrs-cni-bridge {}",
        path
    );
    let output_add = std::process::Command::new("sh")
        .arg("-c")
        .arg(&add_command)
        .output();
    match output_add {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let json: Value = match serde_json::from_str(&stdout) {
                Ok(json) => json,
                Err(e) => {
                    return Err(Box::new(e));
                }
            };
            if let Some(ips) = json.get("ips").and_then(|ips| ips.as_array()) {
                if let Some(first_ip) = ips
                    .get(0)
                    .and_then(|ip| ip.get("address"))
                    .and_then(|addr| addr.as_str())
                {
                    println!("Assigned IP: {}", first_ip);

                    ip = first_ip.to_string();
                    // println!("IP saved to container_ip.txt");
                } else {
                    eprintln!("No IP address found in JSON.");
                }
            } else {
                eprintln!("No 'ips' field found in JSON.");
            }
        }
        Err(e) => {
            // eprintln!("Failed to execute cnitool: {}", e);
            return Err(Box::new(e));
        }
    }

    Ok((ip, path))
}

pub fn delete_cni_network(ns: &str, cid: &str) {
    let netns = get_netns(ns, cid);
    let path = get_path(&netns);
    //todo 错误处理
    let del_command = format!(
        "export CNI_PATH=/opt/cni/bin && cnitool del faasrs-cni-bridge {}",
        path
    );
    let _output_del = std::process::Command::new("sh")
        .arg("-c")
        .arg(&del_command)
        .output()
        .expect("Failed to execute del command");
    let _output = std::process::Command::new("ip")
        .arg("netns")
        .arg("delete")
        .arg(&netns)
        .output();
}

fn cni_gateway() -> Result<String, Err> {
    let ip: IpAddr = DEFAULT_SUBNET.parse().unwrap();
    if let IpAddr::V4(ip) = ip {
        let octets = &mut ip.octets();
        octets[3] = 1;
        return Ok(ip.to_string());
    }
    Err(Box::new(Error))
}
fn dir_exists(dirname: &Path) -> bool {
    path_exists(dirname).map_or(false, |info| info.is_dir())
}

fn dir_empty(dirname: &Path) -> bool {
    if !dir_exists(dirname) {
        return false;
    }
    match fs::read_dir(dirname) {
        Ok(mut entries) => entries.next().is_none(),
        Err(_) => false,
    }
}

fn path_exists(path: &Path) -> Option<fs::Metadata> {
    match fs::metadata(path) {
        Ok(metadata) => Some(metadata),
        Err(_) => None,
    }
}
