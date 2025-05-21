use chrono::Utc;
use iceberg_ext::catalog::rest::{ErrorModel, IcebergErrorResponse};
use itertools::Itertools;
use sqlx::{postgres::types::PgInterval, PgConnection, PgPool};
use uuid::Uuid;

use crate::{
    implementations::postgres::{dbutils::DBErrorHandler, ReadWrite},
    service::task_queue::{Task, TaskFilter, TaskQueue, TaskStatus},
};

#[derive(Debug, Clone)]
pub struct PgQueue {
    pub read_write: ReadWrite,
}

impl PgQueue {
    /// Creates a new `PgQueue` instance from the provided `ReadWrite` and `TaskQueueConfig`.
    ///
    /// # Errors
    /// Returns an error if the `max_age` duration in the `TaskQueueConfig` is invalid.
    pub fn new(read_write: ReadWrite) -> anyhow::Result<Self> {
        Ok(Self { read_write })
    }
}

#[async_trait::async_trait]
impl TaskQueue for PgQueue {
    async fn pick_new_task(
        &self,
        queue_name: &str,
        max_age: chrono::Duration,
    ) -> crate::api::Result<Option<Task>> {
        pick_task(&self.read_write.write_pool, queue_name, max_age).await
    }

    async fn record_success(&self, id: Uuid, message: Option<&str>) -> crate::api::Result<()> {
        record_success(id, &self.read_write.write_pool, message).await
    }

    async fn record_failure(
        &self,
        id: Uuid,
        error_details: &str,
        max_retries: i32,
    ) -> crate::api::Result<()> {
        record_failure(&self.read_write.write_pool, id, max_retries, error_details).await
    }
}

#[derive(Debug)]
struct InsertResult {
    pub task_id: Uuid,
    pub queue_name: String,
    pub entity_id: EntityId,
}

#[derive(Debug, sqlx::Type, Clone, Copy)]
#[sqlx(type_name = "entity_type", rename_all = "kebab-case")]
enum EntityType {
    Tabular,
}

impl EntityId {
    pub fn entity_type(&self) -> EntityType {
        (*self).into()
    }
}

impl From<EntityId> for EntityType {
    fn from(entity_id: EntityId) -> Self {
        match entity_id {
            EntityId::Tabular(_) => Self::Tabular,
        }
    }
}

pub(crate) async fn queue_task_batch(
    conn: &mut PgConnection,
    queue_name: &'static str,
    tasks: Vec<TaskInput>,
) -> Result<Vec<InsertResult>, IcebergErrorResponse> {
    let mut task_ids = Vec::with_capacity(tasks.len());
    let mut parent_task_ids = Vec::with_capacity(tasks.len());
    let mut warehouse_idents = Vec::with_capacity(tasks.len());
    let mut scheduled_fors = Vec::with_capacity(tasks.len());
    let mut entity_ids = Vec::with_capacity(tasks.len());
    let mut entity_types = Vec::with_capacity(tasks.len());
    let mut states = Vec::with_capacity(tasks.len());
    for TaskInput {
        task_metadata:
            TaskMetadata {
                parent_task_id,
                warehouse_id,
                schedule_for,
                entity_id,
            },
        payload,
    } in tasks
    {
        task_ids.push(Uuid::now_v7());
        parent_task_ids.push(parent_task_id);
        warehouse_idents.push(*warehouse_id);
        scheduled_fors.push(schedule_for);
        states.push(payload);
        entity_types.push(EntityType::from(entity_id));
        entity_ids.push(entity_id.to_uuid());
    }

    Ok(sqlx::query!(
        r#"WITH input_rows AS (
            SELECT
                unnest($1::uuid[]) as task_id,
                $2 as queue_name,
                unnest($3::uuid[]) as parent_task_id,
                unnest($4::uuid[]) as warehouse_id,
                unnest($5::timestamptz[]) as scheduled_for,
                unnest($6::jsonb[]) as payload,
                unnest($7::uuid[]) as entity_ids,
                unnest($8::entity_type[]) as entity_types
        )
        INSERT INTO task(
                task_id,
                queue_name,
                status,
                parent_task_id,
                warehouse_id,
                scheduled_for,
                task_data,
                entity_id,
                entity_type)
        SELECT
            i.task_id,
            i.queue_name,
            $9,
            i.parent_task_id,
            i.warehouse_id,
            i.scheduled_for,
            i.payload,
            i.entity_ids,
            i.entity_types
        FROM input_rows i
        ON CONFLICT (warehouse_id, entity_type, entity_id, queue_name) DO NOTHING
        RETURNING task_id, queue_name, entity_id, entity_type as "entity_type: EntityType""#,
        &task_ids,
        queue_name,
        &parent_task_ids as _,
        &warehouse_idents,
        &scheduled_fors
            .iter()
            .map(|t| t.as_ref())
            .collect::<Vec<_>>() as _,
        &states,
        &entity_ids,
        &entity_types as _,
        TaskStatus::Scheduled as _,
    )
    .fetch_all(conn)
    .await
    .map(|records| {
        records
            .into_iter()
            .map(|record| InsertResult {
                task_id: record.task_id,
                queue_name: record.queue_name,
                entity_id: match record.entity_type {
                    EntityType::Tabular => EntityId::Tabular(record.entity_id),
                },
            })
            .collect_vec()
    })
    .map_err(|e| e.into_error_model("failed queueing tasks"))?)
}

#[tracing::instrument]
async fn pick_task(
    pool: &PgPool,
    queue_name: &str,
    max_age: chrono::Duration,
) -> Result<Option<Task>, IcebergErrorResponse> {
    let max_age = PgInterval {
        months: 0,
        days: 0,
        microseconds: max_age.num_microseconds().ok_or(ErrorModel::internal(
            "Could not convert max_age into microseconds. Integer overflow, this is a bug.",
            "InternalError",
            None,
        ))?,
    };
    let x = sqlx::query!(
r#" WITH updated_task AS (
        SELECT task_id, warehouse_id
        FROM task
        WHERE (status = $4 AND queue_name = $1 AND ((scheduled_for < now() AT TIME ZONE 'UTC') OR (scheduled_for IS NULL)))
                OR (status = $5 AND (now() - picked_up_at) > $3)
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    ),
    cfg AS (SELECT config from task_config WHERE queue_name = $1 and warehouse_id = any(select warehouse_id from updated_task))
    UPDATE task
    SET status = $5, picked_up_at = $2, attempt = task.attempt + 1
    FROM updated_task
    WHERE task.task_id = updated_task.task_id
    RETURNING task.task_id, task.entity_id, task.entity_type as "entity_type: EntityType", task.warehouse_id, task.task_data, task.scheduled_for, task.status as "status: TaskStatus", task.picked_up_at, task.attempt, task.parent_task_id, task.queue_name, (select config from cfg)
    "#,
        queue_name,
        Utc::now(),
        max_age,
        TaskStatus::Scheduled as _,
        TaskStatus::Running as _,
    )
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            tracing::error!(?e, "Failed to pick a task");
            e.into_error_model(format!("Failed to pick a '{queue_name}' task")) })?;

    if let Some(task) = x {
        tracing::trace!("Picked up task: {:?}", task);
        return Ok(Some(Task {
            task_metadata: TaskMetadata {
                warehouse_id: task.warehouse_id.into(),
                entity_id: match task.entity_type {
                    EntityType::Tabular => EntityId::Tabular(task.entity_id),
                },
                parent_task_id: task.parent_task_id,
                schedule_for: Some(task.scheduled_for),
            },
            config: task.config,
            task_id: task.task_id,
            status: task.status,
            queue_name: task.queue_name,
            picked_up_at: task.picked_up_at,
            attempt: task.attempt,
            state: task.task_data,
        }));
    }

    Ok(None)
}

async fn record_success(
    id: Uuid,
    pool: &PgPool,
    message: Option<&str>,
) -> Result<(), IcebergErrorResponse> {
    let _ = sqlx::query!(
        r#"
        WITH history as (
            INSERT INTO task_log(task_id, warehouse_id, queue_name, task_data, status, entity_id, entity_type, message, attempt, started_at, duration)
                SELECT task_id, warehouse_id, queue_name, task_data, $3, entity_id, entity_type, $2, attempt, picked_up_at, now() - picked_up_at FROM task
                                                                        WHERE task_id = $1)
        DELETE FROM task
        WHERE task_id = $1
        "#,
        id,
        message,
        TaskOutcome::Success as _,
    )
    .execute(pool)
    .await
    .map_err(|e| e.into_error_model("failed to record task success"))?;
    Ok(())
}

async fn record_failure(
    conn: &PgPool,
    id: Uuid,
    n_retries: i32,
    details: &str,
) -> Result<(), IcebergErrorResponse> {
    let should_fail = sqlx::query_scalar!(
        r#"
        SELECT attempt >= $1 as "should_fail!"
        FROM task
        WHERE task_id = $2
        "#,
        n_retries,
        id
    )
    .fetch_optional(conn)
    .await
    .map_err(|e| e.into_error_model("failed to check if task should fail"))?
    .unwrap_or(false);

    if should_fail {
        sqlx::query!(
            r#"
            WITH history as (
                INSERT INTO task_log(task_id, warehouse_id, queue_name, task_data, status, entity_id, entity_type, attempt, started_at, duration)
                SELECT task_id, warehouse_id, queue_name, task_data, $2, entity_id, entity_type, attempt, picked_up_at, now() - picked_up_at
                FROM task WHERE task_id = $1
            )
            DELETE FROM task
            WHERE task_id = $1
            "#,
            id,
            TaskOutcome::Failed as _,
        )
        .execute(conn)
        .await
        .map_err(|e| e.into_error_model("failed to log and delete failed task"))?;
    } else {
        sqlx::query!(
            r#"
            WITH task_log as (
                INSERT INTO task_log(task_id, warehouse_id, queue_name, task_data, status, entity_id, entity_type, message, attempt, started_at, duration)
                SELECT task_id, warehouse_id, queue_name, task_data, $4, entity_id, entity_type, $2, attempt, picked_up_at, now() - picked_up_at
                FROM task WHERE task_id = $1
            )
            UPDATE task
            SET status = $3
            WHERE task_id = $1
            "#,
            id,
            details,
            TaskStatus::Scheduled as _,
            TaskOutcome::Failed as _,
        )
        .execute(conn)
        .await
        .map_err(|e| e.into_error_model("failed to update task status"))?;
    }

    Ok(())
}

use crate::service::task_queue::{EntityId, TaskInput, TaskMetadata, TaskOutcome};

/// Cancel pending tasks for a warehouse
/// If `task_ids` are provided in `filter` which are not pending, they are ignored
pub(crate) async fn cancel_pending_tasks(
    connection: &mut PgConnection,
    filter: TaskFilter,
    queue_name: &str,
) -> crate::api::Result<()> {
    match filter {
        TaskFilter::WarehouseId(warehouse_id) => {
            sqlx::query!(
                r#"WITH log as (
                        INSERT INTO task_log(task_id, warehouse_id, queue_name, task_data, entity_id, entity_type, status, attempt, started_at, duration)
                        SELECT task_id, warehouse_id, queue_name, task_data, entity_id, entity_type, $4, attempt, picked_up_at, now() - picked_up_at
                        FROM task
                        WHERE status = $3 AND warehouse_id = $1 AND queue_name = $2
                    )
                    DELETE FROM task
                    WHERE status = $3 AND warehouse_id = $1 AND queue_name = $2
                "#,
                *warehouse_id,
                queue_name,
                TaskStatus::Scheduled as _,
                TaskOutcome::Cancelled as _,
            )
            .fetch_all(connection)
            .await
            .map_err(|e| {
                tracing::error!(
                    ?e,
                    "Failed to cancel {queue_name} Tasks for warehouse {warehouse_id}"
                );
                e.into_error_model(format!(
                    "Failed to cancel {queue_name} Tasks for warehouse {warehouse_id}"
                ))
            })?;
        }
        TaskFilter::TaskIds(task_ids) => {
            sqlx::query!(
                r#"WITH log as (
                        INSERT INTO task_log(task_id, warehouse_id, queue_name, task_data, status, entity_id, entity_type, attempt, started_at, duration)
                        SELECT task_id, warehouse_id, queue_name, task_data, $2, entity_id, entity_type, attempt, picked_up_at, now() - picked_up_at
                        FROM task
                        WHERE status = $3 AND task_id = ANY($1)
                    )
                    DELETE FROM task
                    WHERE status = $3
                    AND task_id = ANY($1)
                "#,
                &task_ids.iter().map(|s| **s).collect::<Vec<_>>(),
                TaskOutcome::Cancelled as _,
                TaskStatus::Scheduled as _,
            )
            .fetch_all(connection)
            .await
            .map_err(|e| {
                tracing::error!(?e, "Failed to cancel Tasks for task_ids {task_ids:?}");
                e.into_error_model("Failed to cancel Tasks for specified ids")
            })?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod test {
    use chrono::DateTime;
    use sqlx::PgPool;
    use uuid::Uuid;

    use super::*;
    use crate::{
        api::management::v1::warehouse::TabularDeleteProfile,
        service::{
            authz::AllowAllAuthorizer,
            task_queue::{EntityId, TaskId, TaskInput, TaskQueueConfig, TaskStatus},
        },
        WarehouseId,
    };

    async fn queue_task(
        conn: &mut PgConnection,
        queue_name: &'static str,
        parent_task_id: Option<Uuid>,
        entity_id: EntityId,
        warehouse_id: WarehouseId,
        schedule_for: Option<DateTime<Utc>>,
        payload: Option<serde_json::Value>,
    ) -> Result<Option<Uuid>, IcebergErrorResponse> {
        Ok(queue_task_batch(
            conn,
            queue_name,
            vec![TaskInput {
                task_metadata: TaskMetadata {
                    warehouse_id,
                    parent_task_id,
                    entity_id,
                    schedule_for,
                },
                payload: payload.unwrap_or(serde_json::json!({})),
            }],
        )
        .await?
        .pop()
        .map(|x| x.task_id))
    }

    #[sqlx::test]
    async fn test_queue_task(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (_, warehouse_id) = setup(pool.clone(), config).await;

        let entity_id = EntityId::Tabular(Uuid::now_v7());
        let id = queue_task(&mut conn, "test", None, entity_id, warehouse_id, None, None)
            .await
            .unwrap();

        assert!(
            queue_task(&mut conn, "test", None, entity_id, warehouse_id, None, None)
                .await
                .unwrap()
                .is_none()
        );

        let id3 = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap();

        assert_ne!(id, id3);
    }

    pub(crate) async fn setup(pool: PgPool, config: TaskQueueConfig) -> (PgQueue, WarehouseId) {
        let prof = crate::tests::test_io_profile();
        let (_, wh) = crate::tests::setup(
            pool.clone(),
            prof,
            None,
            AllowAllAuthorizer,
            TabularDeleteProfile::Hard {},
            None,
            1,
        )
        .await;
        (
            PgQueue::new(ReadWrite::from_pools(pool.clone(), pool)).unwrap(),
            wh.warehouse_id,
        )
    }

    #[sqlx::test]
    async fn test_failed_tasks_are_put_back(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;
        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&pool, id, 5, "test").await.unwrap();

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_failure(&pool, id, 2, "test").await.unwrap();

        assert!(pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .is_none());
    }

    #[sqlx::test]
    async fn test_success_task_arent_polled(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        record_success(id, &pool, Some("")).await.unwrap();

        assert!(pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .is_none());
    }

    #[sqlx::test]
    async fn test_success_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig {
            max_retries: 1,
            ..TaskQueueConfig::default()
        };
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let entity = EntityId::Tabular(Uuid::now_v7());

        let id = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"a": "a"}));

        record_success(task.task_id, &pool, Some("")).await.unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"b": "b"})),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_cancelled_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let entity = EntityId::Tabular(Uuid::now_v7());

        let id = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        cancel_pending_tasks(
            &mut conn,
            TaskFilter::TaskIds(vec![TaskId::from(id)]),
            "test",
        )
        .await
        .unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"b": "b"})),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_failed_tasks_can_be_reinserted(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig {
            max_retries: 1,
            ..TaskQueueConfig::default()
        };
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let entity = EntityId::Tabular(Uuid::now_v7());

        let id = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"a": "a"})),
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"a": "a"}));

        record_failure(&pool, task.task_id, 1, "failed")
            .await
            .unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
            None,
            entity,
            warehouse_id,
            None,
            Some(serde_json::json!({"b": "b"})),
        )
        .await
        .unwrap()
        .unwrap();
        assert_ne!(id, id2);

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.task_id, id2);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
        assert_eq!(task.state, serde_json::json!({"b": "b"}));
    }

    #[sqlx::test]
    async fn test_scheduled_tasks_are_polled_later(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            Some(Utc::now() + chrono::Duration::milliseconds(500)),
            None,
        )
        .await
        .unwrap()
        .unwrap();

        assert!(pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .is_none());

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
    }

    #[sqlx::test]
    async fn test_stale_tasks_are_picked_up_again(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig {
            max_age: chrono::Duration::milliseconds(500),
            ..Default::default()
        };
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        tokio::time::sleep(std::time::Duration::from_millis(600)).await;

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 2);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");
    }

    #[sqlx::test]
    async fn test_multiple_tasks(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let id = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let id2 = queue_task(
            &mut conn,
            "test",
            None,
            EntityId::Tabular(Uuid::now_v7()),
            warehouse_id,
            None,
            None,
        )
        .await
        .unwrap()
        .unwrap();

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", config.max_age)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

        record_success(task.task_id, &pool, Some("")).await.unwrap();
        record_success(id2, &pool, Some("")).await.unwrap();
    }

    #[sqlx::test]
    async fn test_queue_batch(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;

        let ids = queue_task_batch(
            &mut conn,
            "test",
            vec![
                TaskInput {
                    task_metadata: TaskMetadata {
                        entity_id: EntityId::Tabular(Uuid::now_v7()),
                        warehouse_id,
                        parent_task_id: None,
                        schedule_for: None,
                    },

                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: TaskMetadata {
                        entity_id: EntityId::Tabular(Uuid::now_v7()),
                        warehouse_id,
                        parent_task_id: None,
                        schedule_for: None,
                    },
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();
        let id = ids[0].task_id;
        let id2 = ids[1].task_id;

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", config.max_age)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

        record_success(task.task_id, &pool, Some("")).await.unwrap();
        record_success(id2, &pool, Some("")).await.unwrap();
    }

    fn task_metadata(warehouse_id: WarehouseId, entity_id: EntityId) -> TaskMetadata {
        TaskMetadata {
            entity_id,
            warehouse_id,
            parent_task_id: None,
            schedule_for: None,
        }
    }

    #[sqlx::test]
    async fn test_queue_batch_idempotency(pool: PgPool) {
        let mut conn = pool.acquire().await.unwrap();
        let config = TaskQueueConfig::default();
        let (queue, warehouse_id) = setup(pool.clone(), config).await;
        let idp1 = EntityId::Tabular(Uuid::now_v7());
        let idp2 = EntityId::Tabular(Uuid::now_v7());
        let ids = queue_task_batch(
            &mut conn,
            "test",
            vec![
                TaskInput {
                    task_metadata: task_metadata(warehouse_id, idp1),
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: task_metadata(warehouse_id, idp2),
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();

        let id = ids[0].task_id;
        let id2 = ids[1].task_id;

        let task = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        let task2 = pick_task(&pool, "test", config.max_age)
            .await
            .unwrap()
            .unwrap();
        assert!(
            pick_task(&pool, "test", config.max_age)
                .await
                .unwrap()
                .is_none(),
            "There are no tasks left, something is wrong."
        );

        assert_eq!(task.task_id, id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert_eq!(task2.task_id, id2);
        assert!(matches!(task2.status, TaskStatus::Running));
        assert_eq!(task2.attempt, 1);
        assert!(task2.picked_up_at.is_some());
        assert!(task2.task_metadata.parent_task_id.is_none());
        assert_eq!(&task2.queue_name, "test");

        // Re-insert the first task with the same idempotency key
        // and a new idempotency key
        // This should create a new task with the new idempotency key
        let new_key = EntityId::Tabular(Uuid::now_v7());
        let ids_second = queue_task_batch(
            &mut conn,
            "test",
            vec![
                TaskInput {
                    task_metadata: task_metadata(warehouse_id, idp1),
                    payload: serde_json::Value::default(),
                },
                TaskInput {
                    task_metadata: task_metadata(warehouse_id, new_key),
                    payload: serde_json::Value::default(),
                },
            ],
        )
        .await
        .unwrap();
        let new_id = ids_second[0].task_id;

        assert_eq!(ids_second.len(), 1);
        assert_eq!(ids_second[0].entity_id, new_key);

        // move both old tasks to done which will clear them from task table and move them into
        // task_log
        record_success(id, &pool, Some("")).await.unwrap();
        record_success(id2, &pool, Some("")).await.unwrap();

        // Re-insert two tasks with previously used idempotency keys
        let ids_third = queue_task_batch(
            &mut conn,
            "test",
            vec![TaskInput {
                task_metadata: task_metadata(warehouse_id, idp1),
                payload: serde_json::Value::default(),
            }],
        )
        .await
        .unwrap();

        assert_eq!(ids_third.len(), 1);
        let id = ids_third[0].task_id;

        record_success(id, &pool, Some("")).await.unwrap();

        // pick one new task, one re-inserted task
        let task = pick_task(&pool, "test", &queue.max_age)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(task.task_id, new_id);
        assert!(matches!(task.status, TaskStatus::Running));
        assert_eq!(task.attempt, 1);
        assert!(task.picked_up_at.is_some());
        assert!(task.task_metadata.parent_task_id.is_none());
        assert_eq!(&task.queue_name, "test");

        assert!(
            pick_task(&pool, "test", config.max_age)
                .await
                .unwrap()
                .is_none(),
            "There should be no tasks left, something is wrong."
        );

        record_success(task.task_id, &pool, Some("")).await.unwrap();
    }
}
