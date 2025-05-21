use std::{collections::HashMap, fmt::Debug, ops::Deref, sync::Arc, time::Duration};

use chrono::Utc;
use futures::future::BoxFuture;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use strum::EnumIter;
use utoipa::ToSchema;
use uuid::Uuid;

use super::{authz::Authorizer, Transaction, WarehouseId};
use crate::service::{
    task_queue::{
        tabular_expiration_queue::{ExpirationQueue, ExpirationQueueConfig},
        tabular_purge_queue::{PurgeQueue, PurgeQueueConfig},
    },
    Catalog, SecretStore,
};

pub mod tabular_expiration_queue;
pub mod tabular_purge_queue;

pub trait QueueConfig {
    fn max_age(&self) -> chrono::Duration {
        DEFAULT_MAX_AGE
    }
    fn max_retries(&self) -> i32 {
        5
    }
    fn num_workers(&self) -> usize {
        2
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "queue_name", rename_all = "kebab-case")]
pub enum QueueConfigs {
    TabularPurge(PurgeQueueConfig),
    TabularExpiration(ExpirationQueueConfig),
}

impl QueueConfigs {
    /// Converts the queue config to a `serde_json::Value`.
    ///
    /// # Errors
    /// serialization to json fails
    pub fn to_serde_value(&self) -> crate::api::Result<serde_json::Value> {
        Ok(match self {
            QueueConfigs::TabularPurge(cfg) => serde_json::to_value(cfg).map_err(|e| {
                crate::api::ErrorModel::internal(
                    format!("Failed to serialize queue config: {e}"),
                    "QueueConfigSerializationError",
                    Some(Box::new(e)),
                )
            }),
            QueueConfigs::TabularExpiration(cfg) => serde_json::to_value(cfg).map_err(|e| {
                crate::api::ErrorModel::internal(
                    format!("Failed to serialize queue config: {e}"),
                    "QueueConfigSerializationError",
                    Some(Box::new(e)),
                )
            }),
        }?)
    }

    /// Converts a `serde_json::Value` to a queue config.
    ///
    /// # Errors
    /// Unknown queue name or deserialization error
    pub fn from_serde_value(
        queue_name: &str,
        value: serde_json::Value,
    ) -> crate::api::Result<Self> {
        Ok(match queue_name {
            tabular_purge_queue::QUEUE_NAME => {
                QueueConfigs::TabularPurge(serde_json::from_value(value).map_err(|e| {
                    crate::api::ErrorModel::internal(
                        format!("Failed to deserialize queue config: {e}"),
                        "QueueConfigDeserializationError",
                        Some(Box::new(e)),
                    )
                })?)
            }
            tabular_expiration_queue::QUEUE_NAME => {
                QueueConfigs::TabularExpiration(serde_json::from_value(value).map_err(|e| {
                    crate::api::ErrorModel::internal(
                        format!("Failed to deserialize queue config: {e}"),
                        "QueueConfigDeserializationError",
                        Some(Box::new(e)),
                    )
                })?)
            }
            _ => {
                return Err(crate::api::ErrorModel::bad_request(
                    "Invalid queue name",
                    "InvalidQueueName",
                    None,
                )
                .into());
            }
        })
    }
}

impl QueueConfigs {
    #[must_use]
    pub fn queue_name(&self) -> &'static str {
        match self {
            QueueConfigs::TabularPurge(_) => tabular_purge_queue::QUEUE_NAME,
            QueueConfigs::TabularExpiration(_) => tabular_expiration_queue::QUEUE_NAME,
        }
    }
}

#[async_trait::async_trait]
pub trait TaskQueue: Clone {
    type Config: Send
        + Sync
        + 'static
        + Clone
        + DeserializeOwned
        + Serialize
        + ToSchema
        + Default
        + QueueConfig;
    async fn run(&self);
}

pub type TaskQueueProducer = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync + 'static>;

pub const DEFAULT_CHANNEL_SIZE: usize = 1000;

#[derive(Clone)]
struct RegisteredQueue {
    pub queue_task: TaskQueueProducer,
    num_workers: usize,
}

impl Debug for RegisteredQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredQueue")
            .field("queue_task", &"Fn(...)")
            .finish()
    }
}

#[derive(Clone, Debug, Default)]
pub struct TaskQueues {
    registered_queues: HashMap<&'static str, RegisteredQueue>,
}

impl TaskQueues {
    #[must_use]
    pub fn new() -> Self {
        Self {
            registered_queues: HashMap::new(),
        }
    }

    pub fn register_queue<T: TaskQueue + Sync + Send + 'static>(
        &mut self,
        queue_name: &'static str,
        queue_task: T,
        num_workers: usize,
    ) {
        self.registered_queues.insert(
            queue_name,
            RegisteredQueue {
                num_workers,
                queue_task: Arc::new(move || {
                    let clone = queue_task.clone();
                    Box::pin(async move { clone.run().await })
                }),
            },
        );
    }

    async fn run(mut self) -> anyhow::Result<()> {
        let mut queue_tasks = vec![];
        let mut qs = HashMap::with_capacity(0);
        std::mem::swap(&mut self.registered_queues, &mut qs);
        for (
            name,
            RegisteredQueue {
                num_workers,
                queue_task: task_fn,
            },
        ) in qs
        {
            tracing::info!("Starting task queue {name} with {} workers", num_workers);
            for n in 0..num_workers {
                tracing::debug!("Starting task queue {name} worker {n}");
                let task_fut = task_fn();
                queue_tasks.push(tokio::task::spawn(task_fut));
            }
        }
        let (res, index, _) = futures::future::select_all(queue_tasks).await;
        if let Err(e) = res {
            tracing::error!("Task queue {index} panicked: {e}");
            return Err(anyhow::anyhow!("Task queue {index} panicked: {e}"));
        }
        tracing::error!("Task queue {index} exited unexpectedly");
        Err(anyhow::anyhow!("Task queue {index} exited unexpectedly"))
    }

    /// Spawns the built-in queues, currently `tabular_expiration_queue` and `tabular_purge_queue` alongside any
    /// registered custom queues.
    ///
    /// # Errors
    /// Fails if any of the queue handlers exit unexpectedly.
    pub async fn spawn_queues<C, S, A>(
        mut self,
        catalog_state: C::State,
        secret_store: S,
        authorizer: A,
        poll_interval: std::time::Duration,
    ) -> Result<(), anyhow::Error>
    where
        C: Catalog,
        S: SecretStore,
        A: Authorizer,
    {
        // Arc::new(move || {
        //     let catalog_state = catalog_state_clone.clone();
        //     let authorizer = authorizer.clone();
        //     async move {
        //         tabular_expiration_queue::tabular_expiration_task::<C, A>(
        //             catalog_state,
        //             authorizer,
        //             poll_interval,
        //         )
        //             .await;
        //         Ok(())
        //     }
        //         .boxed()
        // })
        let catalog_state_clone = catalog_state.clone();
        self.register_queue(
            tabular_expiration_queue::QUEUE_NAME,
            ExpirationQueue::<C, A> {
                catalog_state: catalog_state.clone(),
                authz: authorizer,
                poll_interval,
            },
            2,
        );
        // Arc::new(move || {
        //     let catalog_state = catalog_state.clone();
        //     let secret_store = secret_store.clone();
        //     async move {
        //         tabular_purge_queue::purge_task::<C, S>(
        //             catalog_state,
        //             secret_store,
        //             poll_interval,
        //         )
        //             .await;
        //         Ok(())
        //     }
        //         .boxed()
        // })
        self.register_queue(
            tabular_purge_queue::QUEUE_NAME,
            PurgeQueue::<C, S> {
                catalog_state: catalog_state_clone,
                secret_state: secret_store.clone(),
                poll_interval,
            },
            2,
        );
        self.run().await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Copy)]
pub struct TaskId(Uuid);

impl std::fmt::Display for TaskId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Uuid> for TaskId {
    fn from(id: Uuid) -> Self {
        Self(id)
    }
}

impl From<TaskId> for Uuid {
    fn from(id: TaskId) -> Self {
        id.0
    }
}

impl Deref for TaskId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// A filter to select tasks
#[derive(Debug, Clone, PartialEq)]
pub enum TaskFilter {
    WarehouseId(WarehouseId),
    TaskIds(Vec<TaskId>),
}

#[derive(Debug, Clone)]
pub struct TaskInput {
    pub task_metadata: TaskMetadata,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TaskMetadata {
    pub warehouse_id: WarehouseId,
    pub parent_task_id: Option<TaskId>,
    pub entity_id: EntityId,
    pub schedule_for: Option<chrono::DateTime<Utc>>,
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub enum EntityId {
    Tabular(Uuid),
}

impl EntityId {
    #[must_use]
    pub fn to_uuid(&self) -> Uuid {
        match self {
            EntityId::Tabular(id) => *id,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub task_metadata: TaskMetadata,
    pub queue_name: String,
    pub task_id: TaskId,
    pub status: TaskStatus,
    pub picked_up_at: Option<chrono::DateTime<Utc>>,
    pub attempt: i32,
    pub(crate) config: Option<serde_json::Value>,
    pub(crate) state: serde_json::Value,
}

#[derive(Debug, Clone, Copy)]
pub enum TaskCheckState {
    Stop,
    Continue,
}

impl Task {
    /// Extracts the task state from the task.
    ///
    /// # Errors
    /// Returns an error if the task state cannot be deserialized into the specified type.
    pub fn task_state<T: DeserializeOwned>(&self) -> crate::api::Result<T> {
        Ok(serde_json::from_value(self.state.clone()).map_err(|e| {
            crate::api::ErrorModel::internal(
                format!("Failed to deserialize task state: {e}"),
                "TaskStateDeserializationError",
                Some(Box::new(e)),
            )
        })?)
    }

    /// Extracts the task configuration from the task.
    ///
    /// # Errors
    /// Returns an error if the task configuration cannot be deserialized into the specified type.
    pub fn task_config<T: DeserializeOwned>(&self) -> crate::api::Result<Option<T>> {
        Ok(self
            .config
            .as_ref()
            .map(|cfg| {
                serde_json::from_value(cfg.clone()).map_err(|e| {
                    crate::api::ErrorModel::internal(
                        format!("Failed to deserialize task config: {e}"),
                        "TaskConfigDeserializationError",
                        Some(Box::new(e)),
                    )
                })
            })
            .transpose()?)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, EnumIter)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "task_intermediate_status", rename_all = "kebab-case")
)]
pub enum TaskStatus {
    Scheduled,
    Running,
    ShouldStop,
}

#[derive(Debug, Copy, Clone, PartialEq)]
#[cfg_attr(feature = "sqlx-postgres", derive(sqlx::Type))]
#[cfg_attr(
    feature = "sqlx-postgres",
    sqlx(type_name = "task_final_status", rename_all = "kebab-case")
)]
pub enum TaskOutcome {
    Failed,
    Cancelled,
    Completed,
}

#[derive(Debug)]
pub enum Status<'a> {
    Success(Option<&'a str>),
    Failure(&'a str, i32),
}

impl std::fmt::Display for Status<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Status::Success(details) => write!(f, "success ({})", details.unwrap_or("")),
            Status::Failure(details, _) => write!(f, "failure ({details})"),
        }
    }
}

pub(crate) async fn record_error_with_catalog<C: Catalog>(
    catalog_state: C::State,
    error: &str,
    max_retries: i32,
    task_id: TaskId,
) {
    let mut trx: C::Transaction = match Transaction::begin_write(catalog_state).await.map_err(|e| {
        tracing::error!("Failed to start transaction: {:?}", e);
        e
    }) {
        Ok(trx) => trx,
        Err(e) => {
            tracing::error!("Failed to start transaction: {:?}", e);
            return;
        }
    };
    C::retrying_record_task_failure(task_id, error, max_retries, trx.transaction()).await;
    let _ = trx.commit().await.inspect_err(|e| {
        tracing::error!("Failed to commit transaction: {:?}", e);
    });
}

// #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
// pub struct TaskQueueConfig {
//     pub max_retries: i32,
//     #[serde(
//         deserialize_with = "crate::config::seconds_to_duration",
//         serialize_with = "crate::config::duration_to_seconds"
//     )]
//     pub max_age: chrono::Duration,
//     #[serde(
//         deserialize_with = "crate::config::seconds_to_std_duration",
//         serialize_with = "crate::config::serialize_std_duration_as_ms"
//     )]
//     pub poll_interval: Duration,
//     pub num_workers: usize,
// }

pub const DEFAULT_MAX_AGE: chrono::Duration = valid_max_age(3600);
pub const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(10);

// impl Default for TaskQueueConfig {
//     fn default() -> Self {
//         Self {
//             max_retries: 5,
//             max_age: valid_max_age(3600),
//             poll_interval: Duration::from_secs(10),
//             num_workers: 2,
//         }
//     }
// }

const fn valid_max_age(num: i64) -> chrono::Duration {
    assert!(num > 0, "max_age must be greater than 0");
    let dur = chrono::Duration::seconds(num);
    assert!(dur.num_microseconds().is_some());
    dur
}

#[cfg(test)]
mod test {

    use sqlx::PgPool;
    use tracing_test::traced_test;

    use crate::{
        api::{
            iceberg::v1::PaginationQuery,
            management::v1::{DeleteKind, TabularType},
        },
        implementations::postgres::{
            tabular::table::tests::initialize_table, warehouse::test::initialize_warehouse,
            CatalogState, PostgresCatalog, PostgresTransaction,
        },
        service::{
            authz::AllowAllAuthorizer,
            storage::TestProfile,
            task_queue::{tabular_expiration_queue::TabularExpiration, EntityId, TaskMetadata},
            Catalog, ListFlags, Transaction,
        },
    };

    // #[cfg(feature = "sqlx-postgres")]
    #[sqlx::test]
    #[traced_test]
    async fn test_queue_expiration_queue_task(pool: PgPool) {
        let catalog_state = CatalogState::from_pools(pool.clone(), pool.clone());

        let queues = crate::service::task_queue::TaskQueues::new();

        let secrets =
            crate::implementations::postgres::SecretsState::from_pools(pool.clone(), pool);
        let cat = catalog_state.clone();
        let sec = secrets.clone();
        let auth = AllowAllAuthorizer;
        let _queue_task = tokio::task::spawn(
            queues.spawn_queues::<PostgresCatalog, _, AllowAllAuthorizer>(
                cat,
                sec,
                auth,
                std::time::Duration::from_millis(100),
            ),
        );

        let warehouse = initialize_warehouse(
            catalog_state.clone(),
            Some(TestProfile::default().into()),
            None,
            None,
            true,
        )
        .await;

        let tab = initialize_table(
            warehouse,
            catalog_state.clone(),
            false,
            None,
            Some("tab".to_string()),
        )
        .await;
        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();
        let _ = <PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: true,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .unwrap();
        trx.commit().await.unwrap();
        let mut trx = <PostgresCatalog as Catalog>::Transaction::begin_write(catalog_state.clone())
            .await
            .unwrap();
        let _ = PostgresCatalog::queue_tabular_expiration(
            TaskMetadata {
                warehouse_id: warehouse,
                entity_id: EntityId::Tabular(tab.table_id.0),
                parent_task_id: None,
                schedule_for: Some(chrono::Utc::now() + chrono::Duration::seconds(1)),
            },
            TabularExpiration {
                tabular_type: TabularType::Table,
                deletion_kind: DeleteKind::Purge,
            },
            trx.transaction(),
        )
        .await
        .unwrap();

        <PostgresCatalog as Catalog>::mark_tabular_as_deleted(
            tab.table_id.into(),
            false,
            trx.transaction(),
        )
        .await
        .unwrap();

        trx.commit().await.unwrap();

        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();

        let del = <PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .unwrap()
        .deletion_details;
        del.unwrap();
        trx.commit().await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(1250)).await;

        let mut trx = PostgresTransaction::begin_read(catalog_state.clone())
            .await
            .unwrap();

        assert!(<PostgresCatalog as Catalog>::list_tabulars(
            warehouse,
            None,
            ListFlags {
                include_active: false,
                include_staged: false,
                include_deleted: true,
            },
            trx.transaction(),
            PaginationQuery::empty(),
        )
        .await
        .unwrap()
        .remove(&tab.table_id.into())
        .is_none());
        trx.commit().await.unwrap();
    }
}
