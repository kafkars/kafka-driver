//! Modeled connection boundary retained beside the production broker owner.

use crate::reactor::resource::ResourceToken;

use super::SingleBroker;

impl SingleBroker {
    pub(in crate::reactor) fn enable_simulation(&mut self) {
        self.resources.enable_simulation();
    }

    pub(in crate::reactor) fn simulate_connect(&mut self) -> Option<ResourceToken> {
        let token = self.resource_token?;
        self.resources.simulate_connect(token).then_some(token)
    }

    pub(in crate::reactor) fn simulate_receive(&mut self, bytes: Vec<u8>) -> Option<ResourceToken> {
        let token = self.resource_token?;
        self.resources
            .simulate_receive(token, bytes)
            .then_some(token)
    }

    pub(in crate::reactor) fn take_simulated_frames(&mut self) -> Vec<Vec<u8>> {
        let Some(token) = self.resource_token else {
            return Vec::new();
        };
        self.resources.take_simulated_frames(token)
    }
}
