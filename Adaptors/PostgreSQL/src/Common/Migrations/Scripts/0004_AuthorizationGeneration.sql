-- Generation of the grants and permission sets: the last change that began, and the last that ended

CREATE TABLE IF NOT EXISTS authorization_generation (
    id    INTEGER PRIMARY KEY,
    begun BIGINT NOT NULL,
    ended BIGINT NOT NULL
);
