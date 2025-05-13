ALLOWED_TABLES = {
    'events_view',
    'organizations_view',
    'top_volunteers_view',
    'volunteers_view',
    'results_view'
}

BLACKLISTED_COLUMNS = {
    'events.approved',
    'organizations.password', 'organizations.approved',
    'volunteers.password'
}
