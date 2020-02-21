table! {
    messages (id) {
        id -> Uuid,
        payload -> Bytea,
        content_type -> Varchar,
        hash -> Nullable<Varchar>,
        queue -> Varchar,
        receives -> Int4,
        visible_since -> Timestamp,
        created_at -> Timestamp,
    }
}

table! {
    queues (id) {
        id -> Int4,
        name -> Varchar,
        max_receives -> Nullable<Int4>,
        dead_letter_queue -> Nullable<Varchar>,
        retention_timeout -> Interval,
        visibility_timeout -> Interval,
        message_delay -> Interval,
        content_based_deduplication -> Bool,
        created_at -> Timestamp,
        updated_at -> Timestamp,
    }
}

allow_tables_to_appear_in_same_query!(
    messages,
    queues,
);
