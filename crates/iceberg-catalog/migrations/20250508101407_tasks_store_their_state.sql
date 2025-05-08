create type entity_type as enum ('tabular');
create type task_outcomes as enum ('failure', 'cancellation', 'success');

alter table task
    add column state       jsonb,
    add column entity_type entity_type,
    add column entity_id   uuid
;

update task
set state       = jsonb_build_object(
        'tabular_id', te.tabular_id,
        'typ', te.typ,
        'deletion_kind', te.deletion_kind),
    entity_type = 'tabular',
    entity_id   = te.tabular_id
from tabular_expirations te
where te.task_id = task.task_id;

update task
set state       = jsonb_build_object(
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
    alter column state set not null,
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
    task_id            uuid primary key                                           not null,
    warehouse_id       uuid references warehouse (warehouse_id) on delete cascade not null,
    queue_name         text                                                       not null,
    state              jsonb                                                      not null,
    status             task_outcomes                                              not null,
    entity_id          uuid                                                       not null,
    entity_type        entity_type                                                not null,
    last_error_details text
);

insert into task_log (task_id, warehouse_id, queue_name, state, status, last_error_details, entity_id, entity_type)
select task.task_id,
       task.warehouse_id,
       task.queue_name,
       task.state,
       CASE
           when task.status = 'cancelled' then 'cancellation'::task_outcomes
           when task.status = 'failed' then 'failure'::task_outcomes
           when task.status = 'done' then 'success'::task_outcomes
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
    drop column idempotency_key;

drop type task_status;
create type task_status as enum ('running', 'pending');
alter table task
    alter column status type task_status using status::task_status;

create index task_queue_name_status_idx on task (queue_name, status) where status = 'pending' OR status = 'running';
create index task_warehouse_queue_name_idx on task (warehouse_id, queue_name);
create index if not exists task_entity_type_entity_id_idx
    on task (entity_type, entity_id);