//! Hub — bridges tunnel protocol traits with the in-memory `ChannelIndex`.

use crate::{
    gateway::{ChannelEntry, ChannelIndex},
    model::Token as DbToken,
};
use std::sync::Arc;
use toasty::Db;
use tokilake_core::{
    error::TunnelError,
    gateway::{Authenticator, WorkerRegistry},
    protocol::{RegisterResult, Token as CoreToken},
};

#[derive(Clone)]
pub struct ToastyHub {
    pub db:    Db,
    pub index: Arc<ChannelIndex>,
}

impl Authenticator for ToastyHub {
    async fn authenticate_token_key(
        &self,
        token_key: &str,
    ) -> Result<(String, CoreToken), TunnelError> {
        let mut db = self.db.clone();
        let token_key = token_key.strip_prefix("sk-").unwrap_or(token_key);

        match DbToken::filter_by_key(token_key)
            .first()
            .exec(&mut db)
            .await
        {
            Ok(Some(t)) if t.status == 1 => Ok((token_key.into(), CoreToken {
                user_id: t.id as i64,
            })),
            _ => Err(TunnelError::protocol("Invalid or disabled token")),
        }
    }
}

impl WorkerRegistry for ToastyHub {
    async fn register_worker(
        &self,
        _session_id: u64,
        namespace: &str,
        node_name: &str,
        group: &str,
        models: &[String],
        backend_type: &str,
    ) -> Result<RegisterResult, TunnelError> {
        let channel_id = self.index.channel_count() as i64 + 1;
        let effective_group = if group.trim().is_empty() {
            "default"
        } else {
            group
        };

        self.index.upsert(ChannelEntry {
            id:            channel_id,
            name:          namespace.into(),
            provider:      backend_type.into(),
            models:        models.join(","),
            base_url:      None,
            api_key:       None,
            status:        1,
            weight:        1,
            group:         effective_group.into(),
            priority:      0,
            model_mapping: None,
        });

        tracing::info!(
            "worker registered: id={channel_id} namespace={namespace} node={node_name} \
             models={models:?}"
        );

        Ok(RegisterResult {
            worker_id:    channel_id as i32,
            channel_id:   channel_id as i32,
            namespace:    namespace.into(),
            group:        effective_group.into(),
            models:       models.to_vec(),
            backend_type: backend_type.into(),
            status:       1,
        })
    }

    async fn update_heartbeat(
        &self,
        worker_id: i32,
        _status: i32,
        _node_name: &str,
        current_models: &[String],
    ) -> Result<(), TunnelError> {
        if !current_models.is_empty() {
            if let Some(mut ch) = self.index.get(worker_id as i64) {
                let new_models = current_models.join(",");
                if ch.models != new_models {
                    ch.models = new_models;
                    self.index.upsert(ch);
                }
            }
        }
        Ok(())
    }

    async fn sync_models(
        &self,
        worker_id: i32,
        _group: &str,
        models: &[String],
        backend_type: &str,
    ) -> Result<(), TunnelError> {
        if let Some(mut ch) = self.index.get(worker_id as i64) {
            ch.models = models.join(",");
            ch.provider = backend_type.into();
            self.index.upsert(ch);
        }
        Ok(())
    }

    async fn cleanup_worker(&self, worker_id: i32) -> Result<(), TunnelError> {
        self.index.remove(worker_id as i64);
        tracing::info!("worker removed: id={worker_id}");
        Ok(())
    }
}
