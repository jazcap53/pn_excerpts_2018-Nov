DROP TABLE IF EXISTS pn_contacts CASCADE;

CREATE TABLE pn_contacts (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    email VARCHAR UNIQUE NOT NULL,
    addr_1 VARCHAR,
    addr_2 VARCHAR,
    city VARCHAR,
    name VARCHAR,
    phone VARCHAR,
    postcode VARCHAR,
    state VARCHAR,
    pgres_last_updated TIMESTAMPTZ
);


DROP TABLE IF EXISTS pn_addons CASCADE;

CREATE TABLE pn_addons (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    key VARCHAR UNIQUE NOT NULL,
    name VARCHAR UNIQUE NOT NULL,
    pgres_last_updated TIMESTAMPTZ
);


DROP TABLE IF EXISTS pn_organizations CASCADE;

CREATE TABLE pn_organizations (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    name VARCHAR NOT NULL,
    primary_role VARCHAR,
    short_description VARCHAR,
    domain VARCHAR UNIQUE NOT NULL,
    homepage_url VARCHAR,
    facebook_url VARCHAR,
    twitter_url VARCHAR,
    linkedin_url VARCHAR,
    api_url VARCHAR,
    city VARCHAR,  -- city name in API
    region VARCHAR,  -- region_name in API
    country VARCHAR,  -- country code in API
    stock_exchange VARCHAR,
    stock_symbol VARCHAR,
    created_at INTEGER,  -- seconds since the epoch
    updated_at INTEGER,  -- seconds since the epoch
    pgres_last_updated TIMESTAMPTZ
);


DROP TABLE IF EXISTS pn_partner_details CASCADE;

CREATE TABLE pn_partner_details (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    name VARCHAR UNIQUE NOT NULL,
    type VARCHAR NOT NULL,
    bill_contact_name VARCHAR,
    bill_contact_email VARCHAR,
    pgres_last_updated TIMESTAMPTZ,
    UNIQUE (name, type)
);


DROP TABLE IF EXISTS pn_license_contact_details CASCADE;

CREATE TABLE pn_license_contact_details (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    company VARCHAR NOT NULL,
    country VARCHAR NOT NULL,
    region VARCHAR NOT NULL,
    bill_contact_id UUID,
    tech_contact_id UUID NOT NULL,
    pgres_last_updated TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (bill_contact_id) REFERENCES pn_contacts (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (tech_contact_id) REFERENCES pn_contacts (id)
    ON UPDATE CASCADE ON DELETE CASCADE ,
    UNIQUE (company, country, region) --, bill_contact_id, tech_contact_id)
);


DROP TABLE IF EXISTS pn_licenses;

CREATE TABLE pn_licenses (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v1mc(),
    license_id VARCHAR UNIQUE NOT NULL,
    addons_id UUID NOT NULL,
    license_contact_details_id UUID,
    partner_details_id UUID,
    organizations_id UUID,
    addon_key VARCHAR NOT NULL,
    hosting VARCHAR NOT NULL,
    host_license_id VARCHAR,
    last_updated DATE NOT NULL,
    license_type VARCHAR NOT NULL,
    maint_start_date TIMESTAMPTZ NOT NULL,
    maint_end_date TIMESTAMPTZ NOT NULL,
    status VARCHAR NOT NULL,
    tier VARCHAR NOT NULL,
    pgres_last_updated TIMESTAMPTZ,
    FOREIGN KEY (addons_id) REFERENCES pn_addons (id),
    FOREIGN KEY (license_contact_details_id) REFERENCES pn_license_contact_details (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (partner_details_id) REFERENCES pn_partner_details (id)
    ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (addon_key) REFERENCES pn_addons (key),
    FOREIGN KEY (organizations_id) REFERENCES pn_organizations (id)
);
