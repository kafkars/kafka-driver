//! Stable-version selection from one validated `ApiVersions` response.

use kafka_driver_core::{NegotiatedApi, NegotiatedCapabilities};
use kafka_wire::{API_DESCRIPTORS, ApiDescriptor, ApiVersionsResponse};
use kafka_wire_core::{ApiKey, ApiVersion};

use super::{NegotiationError, NegotiationLimits};

pub(crate) fn negotiate(
    mut response: ApiVersionsResponse,
    limits: NegotiationLimits,
) -> Result<NegotiatedCapabilities, NegotiationError> {
    if response.error_code != 0 {
        return Err(NegotiationError::BrokerRejected {
            error_code: response.error_code,
        });
    }
    let observed = response.api_keys.len();
    let limit = limits.max_advertised_apis();
    if observed > limit {
        return Err(NegotiationError::AdvertisementCapacity { observed, limit });
    }

    response.api_keys.sort_unstable_by_key(|api| api.api_key);
    let mut previous = None;
    let mut selected = Vec::new();
    for advertised in response.api_keys {
        let api_key = ApiKey::new(advertised.api_key);
        if advertised.min_version > advertised.max_version {
            return Err(NegotiationError::InvalidRange {
                api_key,
                min_version: advertised.min_version,
                max_version: advertised.max_version,
            });
        }
        if previous == Some(api_key) {
            return Err(NegotiationError::DuplicateApi { api_key });
        }
        previous = Some(api_key);
        if let Some(api) = local_descriptor(api_key)
            && let Some(version) =
                highest_stable_overlap(*api, advertised.min_version, advertised.max_version)
        {
            selected.push(NegotiatedApi::new(api_key, version));
        }
    }

    NegotiatedCapabilities::try_from_iter(selected, limits.max_negotiated_apis())
        .map_err(Into::into)
}

fn local_descriptor(api_key: ApiKey) -> Option<&'static ApiDescriptor> {
    API_DESCRIPTORS
        .binary_search_by_key(&api_key, |api| api.api_key)
        .ok()
        .map(|index| &API_DESCRIPTORS[index])
}

fn highest_stable_overlap(
    local: ApiDescriptor,
    broker_min: i16,
    broker_max: i16,
) -> Option<ApiVersion> {
    let local_max = local.latest_stable_version()?.value();
    let overlap_min = local.supported_versions.min().value().max(broker_min);
    let overlap_max = local_max.min(broker_max);
    (overlap_min <= overlap_max).then(|| ApiVersion::new(overlap_max))
}
