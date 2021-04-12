CREATE DATABASE "TDF";
use TDF;
CREATE TABLE country (
    id integer NOT NULL DEFAULT nextval('country_id_seq'::regclass),
    country_code character varying COLLATE pg_catalog."default",
    CONSTRAINT country_pkey PRIMARY KEY (id)
);
CREATE TABLE rider (
    id integer NOT NULL DEFAULT nextval('rider_id_seq'::regclass),
    rider_name character varying COLLATE pg_catalog."default",
    rider_country_id integer NOT NULL,
    CONSTRAINT rider_pkey PRIMARY KEY (id),
    CONSTRAINT rider_rider_country_id_fkey FOREIGN KEY (rider_country_id) REFERENCES country (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);
CREATE TABLE stage_location (
    id integer NOT NULL DEFAULT nextval('stage_location_id_seq'::regclass),
    location_name character varying COLLATE pg_catalog."default",
    latitude double precision,
    longitude double precision,
    CONSTRAINT stage_location_pkey PRIMARY KEY (id),
    CONSTRAINT stage_location_location_name_key UNIQUE (location_name)
);
CREATE TABLE stage_type (
    id integer NOT NULL DEFAULT nextval('stage_type_id_seq'::regclass),
    stage_type character varying COLLATE pg_catalog."default",
    CONSTRAINT stage_type_pkey PRIMARY KEY (id)
);
CREATE TABLE pstage (
    id integer NOT NULL DEFAULT nextval('stage_id_seq'::regclass),
    stage_sequence character varying COLLATE pg_catalog."default",
    date timestamp without time zone,
    distance double precision,
    "origin_ID" integer NOT NULL,
    "destination_ID" integer NOT NULL,
    "stage_Type_ID" integer NOT NULL,
    "winner_ID" integer NOT NULL,
    CONSTRAINT stage_pkey PRIMARY KEY (id),
    CONSTRAINT "stage_destination_ID_fkey" FOREIGN KEY ("destination_ID") REFERENCES stage_location (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT "stage_origin_ID_fkey" FOREIGN KEY ("origin_ID") REFERENCES stage_location (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT "stage_stage_Type_ID_fkey" FOREIGN KEY ("stage_Type_ID") REFERENCES stage_type (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION,
    CONSTRAINT "stage_winner_ID_fkey" FOREIGN KEY ("winner_ID") REFERENCES rider (id) MATCH SIMPLE ON UPDATE NO ACTION ON DELETE NO ACTION
);