create type entity_type as enum ('tabular');
create type task_final_status as enum ('failure', 'cancellation', 'success');

alter table task
    add column task_data   jsonb,
    add column entity_type entity_type,
    add column entity_id   uuid
;

update task
set task_data   = jsonb_build_object(
        'tabular_id', te.tabular_id,
        'typ', te.typ,
        'deletion_kind', te.deletion_kind),
    entity_type = 'tabular',
    entity_id   = te.tabular_id
from tabular_expirations te
where te.task_id = task.task_id;

update task
set task_data   = jsonb_build_object(
        'tabular_id', tp.tabular_id,
        'typ', tp.typ,
        'tabular_location', tp.tabular_location),
    entity_type = 'tabular',
    entity_id   = tp.tabular_id
from tabular_purges tp
where tp.task_id = task.task_id;

drop table tabular_expirations;
drop table tabular_purges;

alter table task
    alter column task_data set not null,
    alter column entity_type set not null,
    alter column entity_id set not null;

create table task_config
(
    warehouse_id uuid references warehouse (warehouse_id) on delete cascade not null,
    queue_name   text                                                       not null,
    config       jsonb                                                      not null,
    primary key (warehouse_id, queue_name)
);


create table task_log
(
    task_id      uuid                                                       not null,
    attempt      integer                                                    not null,
    warehouse_id uuid references warehouse (warehouse_id) on delete cascade not null,
    queue_name   text                                                       not null,
    task_data    jsonb                                                      not null,
    status       task_final_status                                          not null,
    entity_id    uuid                                                       not null,
    entity_type  entity_type                                                not null,
    message      text,
    primary key (task_id, attempt)
);

call add_time_columns('task_log');
select trigger_updated_at('task_log');

create index if not exists task_log_warehouse_id_idx
    on task_log (warehouse_id);
create index if not exists task_log_warehouse_id_entity_id_idx
    on task_log (warehouse_id, entity_id);


insert into task_log (task_id, warehouse_id, attempt, queue_name, task_data, status, message, entity_id,
                      entity_type)
select task.task_id,
       task.warehouse_id,
       task.attempt,
       task.queue_name,
       task.task_data,
       CASE
           when task.status = 'cancelled' then 'cancellation'::task_final_status
           when task.status = 'failed' then 'failure'::task_final_status
           when task.status = 'done' then 'success'::task_final_status
           END,
       task.last_error_details,
       task.entity_id,
       task.entity_type
from task
where task.status != 'running'
  AND task.status != 'pending';

DELETE
FROM task
WHERE task.status != 'running'
  AND task.status != 'pending';

drop index if exists task_queue_name_status_idx;
drop index if exists task_warehouse_queue_name_idx;

alter table task
    alter column status type text,
    drop constraint unique_idempotency_key,
    add constraint unique_entity_type_entity_id_queue_name unique (entity_type, entity_id, queue_name),
    drop column idempotency_key,
    drop column last_error_details;

drop type task_status;
create type task_intermediate_status as enum ('running', 'pending');
alter table task
    alter column status type task_intermediate_status using status::task_intermediate_status;

create index task_queue_name_status_idx on task (queue_name, status);
create index task_warehouse_queue_name_idx on task (warehouse_id, queue_name);
create index if not exists task_entity_type_entity_id_idx
    on task (entity_type, entity_id);