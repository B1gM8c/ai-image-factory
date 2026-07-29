use std::{
    env,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

use reqwest::Url;

use crate::ImageGatewayError;

#[derive(Clone, Debug)]
pub struct WebhookDestinationPolicy {
    allow_http: bool,
    allow_private_networks: bool,
}

#[derive(Clone, Debug)]
pub struct ResolvedWebhookDestination {
    pub url: Url,
    pub host: String,
    pub addresses: Vec<SocketAddr>,
}

impl Default for WebhookDestinationPolicy {
    fn default() -> Self {
        Self {
            allow_http: false,
            allow_private_networks: false,
        }
    }
}

impl WebhookDestinationPolicy {
    pub fn from_env() -> Result<Self, ImageGatewayError> {
        Ok(Self {
            allow_http: parse_bool_env("GATEWAY_WEBHOOK_ALLOW_HTTP")?,
            allow_private_networks: parse_bool_env("GATEWAY_WEBHOOK_ALLOW_PRIVATE_NETWORKS")?,
        })
    }

    pub fn permissive_for_tests() -> Self {
        Self {
            allow_http: true,
            allow_private_networks: true,
        }
    }

    pub async fn resolve(
        &self,
        raw_url: &str,
    ) -> Result<ResolvedWebhookDestination, ImageGatewayError> {
        let url = Url::parse(raw_url).map_err(|_| invalid_url("url must be a valid URL"))?;
        if !url.username().is_empty() || url.password().is_some() {
            return Err(invalid_url("url must not contain credentials"));
        }
        if url.fragment().is_some() {
            return Err(invalid_url("url must not contain a fragment"));
        }
        match url.scheme() {
            "https" => {}
            "http" if self.allow_http => {}
            _ => {
                return Err(invalid_url(
                    "url must use HTTPS unless the development HTTP override is enabled",
                ));
            }
        }
        let host = url
            .host_str()
            .filter(|host| !host.is_empty())
            .ok_or_else(|| invalid_url("url must include a host"))?
            .to_ascii_lowercase();
        let port = url
            .port_or_known_default()
            .ok_or_else(|| invalid_url("url must include a valid port"))?;
        let mut addresses = if let Ok(ip) = host.parse::<IpAddr>() {
            vec![SocketAddr::new(ip, port)]
        } else {
            tokio::net::lookup_host((host.as_str(), port))
                .await
                .map_err(|_| invalid_url("url host could not be resolved"))?
                .collect::<Vec<_>>()
        };
        addresses.sort();
        addresses.dedup();
        if addresses.is_empty() {
            return Err(invalid_url("url host did not resolve to an address"));
        }
        if !self.allow_private_networks
            && addresses
                .iter()
                .any(|address| !is_public_destination(address.ip()))
        {
            return Err(invalid_url(
                "url host must resolve only to public network addresses",
            ));
        }
        Ok(ResolvedWebhookDestination {
            url,
            host,
            addresses,
        })
    }
}

fn parse_bool_env(name: &str) -> Result<bool, ImageGatewayError> {
    match env::var(name).as_deref() {
        Ok("1" | "true" | "TRUE" | "yes" | "YES") => Ok(true),
        Ok("0" | "false" | "FALSE" | "no" | "NO") | Err(env::VarError::NotPresent) => Ok(false),
        Err(env::VarError::NotUnicode(_)) | Ok(_) => Err(ImageGatewayError::config(format!(
            "{name} must be a boolean"
        ))),
    }
}

fn invalid_url(message: &'static str) -> ImageGatewayError {
    ImageGatewayError::invalid_request(message, Some("url".to_string()), "invalid_webhook_url")
}

fn is_public_destination(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => is_public_ipv4(ip),
        IpAddr::V6(ip) => is_public_ipv6(ip),
    }
}

fn is_public_ipv4(ip: Ipv4Addr) -> bool {
    let [a, b, c, d] = ip.octets();
    if ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_unspecified()
        || ip == Ipv4Addr::BROADCAST
    {
        return false;
    }
    !matches!(
        (a, b, c, d),
        (0, _, _, _)
            | (100, 64..=127, _, _)
            | (192, 0, 0, _)
            | (192, 0, 2, _)
            | (198, 18..=19, _, _)
            | (198, 51, 100, _)
            | (203, 0, 113, _)
            | (240..=255, _, _, _)
    )
}

fn is_public_ipv6(ip: Ipv6Addr) -> bool {
    if let Some(mapped) = ip.to_ipv4_mapped() {
        return is_public_ipv4(mapped);
    }
    let segments = ip.segments();
    if ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (segments[0] & 0xfe00) == 0xfc00
        || (segments[0] & 0xffc0) == 0xfe80
    {
        return false;
    }
    !(segments[0] == 0x2001 && segments[1] == 0x0db8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn production_policy_rejects_http_and_private_hosts() {
        let policy = WebhookDestinationPolicy::default();
        assert!(policy.resolve("http://example.com/hook").await.is_err());
        assert!(policy.resolve("https://127.0.0.1/hook").await.is_err());
        assert!(policy.resolve("https://[::1]/hook").await.is_err());
    }

    #[tokio::test]
    async fn development_policy_accepts_loopback_http() {
        let policy = WebhookDestinationPolicy::permissive_for_tests();
        let resolved = policy.resolve("http://127.0.0.1:8080/hook").await.unwrap();
        assert_eq!(resolved.host, "127.0.0.1");
        assert_eq!(resolved.addresses[0].port(), 8080);
    }
}
