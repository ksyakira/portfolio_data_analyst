
-- =====================================================
-- PROJECT: GENDER PAY GAP IN EUROPE (2015-2024)
-- DATABASE: gender_pay_gap
-- ROLE: DATABASE & DOCUMENTATION
-- =====================================================

BEGIN;

-- =====================================================
-- 1. DIMENSION TABLES
-- =====================================================

-- Dimension: Country
CREATE TABLE public.dim_country (
    country_code VARCHAR(2) PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);


-- Dimension: Year
CREATE TABLE public.dim_year (
    year SMALLINT PRIMARY KEY,

    CONSTRAINT chk_dim_year
        CHECK (year BETWEEN 2015 AND 2024)
);


-- Dimension: Industry
CREATE TABLE public.dim_industry (
    industry_code VARCHAR(5) PRIMARY KEY,
    industry_name VARCHAR(200) NOT NULL UNIQUE
);


-- Dimension: Employment Status
CREATE TABLE public.dim_employment_status (
    employment_status_code VARCHAR(10) PRIMARY KEY,
    employment_status_name VARCHAR(100) NOT NULL UNIQUE
);


-- Dimension: Management Position
CREATE TABLE public.dim_management_position (
    management_position_code VARCHAR(20) PRIMARY KEY,
    management_position_name VARCHAR(150) NOT NULL UNIQUE
);


-- =====================================================
-- 2. FACT TABLES
-- =====================================================

-- Fact: Gender Pay Gap
CREATE TABLE public.fact_gender_pay_gap (
    country_code VARCHAR(2) NOT NULL,
    year SMALLINT NOT NULL,
    industry_code VARCHAR(5) NOT NULL,

    pay_gap_percentage NUMERIC(8,3),
    observation_flag VARCHAR(10),
    confidentiality_status VARCHAR(10),

    CONSTRAINT pk_fact_gender_pay_gap
        PRIMARY KEY (country_code, year, industry_code),

    CONSTRAINT fk_pay_gap_country
        FOREIGN KEY (country_code)
        REFERENCES public.dim_country(country_code)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_pay_gap_year
        FOREIGN KEY (year)
        REFERENCES public.dim_year(year)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_pay_gap_industry
        FOREIGN KEY (industry_code)
        REFERENCES public.dim_industry(industry_code)
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);


-- Fact: Employment Gap
CREATE TABLE public.fact_employment_gap (
    country_code VARCHAR(2) NOT NULL,
    year SMALLINT NOT NULL,
    employment_status_code VARCHAR(10) NOT NULL,

    employment_gap_percentage_points NUMERIC(8,3),
    observation_flag VARCHAR(10),

    CONSTRAINT pk_fact_employment_gap
        PRIMARY KEY (
            country_code,
            year,
            employment_status_code
        ),

    CONSTRAINT fk_employment_country
        FOREIGN KEY (country_code)
        REFERENCES public.dim_country(country_code)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_employment_year
        FOREIGN KEY (year)
        REFERENCES public.dim_year(year)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_employment_status
        FOREIGN KEY (employment_status_code)
        REFERENCES public.dim_employment_status(
            employment_status_code
        )
        ON UPDATE CASCADE
        ON DELETE RESTRICT
);


-- Fact: Female Education
CREATE TABLE public.fact_female_education (
    country_code VARCHAR(2) NOT NULL,
    year SMALLINT NOT NULL,

    female_tertiary_education_percentage NUMERIC(8,3),
    observation_flag VARCHAR(10),

    CONSTRAINT pk_fact_female_education
        PRIMARY KEY (country_code, year),

    CONSTRAINT fk_education_country
        FOREIGN KEY (country_code)
        REFERENCES public.dim_country(country_code)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_education_year
        FOREIGN KEY (year)
        REFERENCES public.dim_year(year)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT chk_female_education_percentage
        CHECK (
            female_tertiary_education_percentage
            BETWEEN 0 AND 100
            OR female_tertiary_education_percentage IS NULL
        )
);


-- Fact: Women in Management
CREATE TABLE public.fact_women_management (
    country_code VARCHAR(2) NOT NULL,
    year SMALLINT NOT NULL,
    management_position_code VARCHAR(20) NOT NULL,

    women_management_percentage NUMERIC(8,3),

    CONSTRAINT pk_fact_women_management
        PRIMARY KEY (
            country_code,
            year,
            management_position_code
        ),

    CONSTRAINT fk_management_country
        FOREIGN KEY (country_code)
        REFERENCES public.dim_country(country_code)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_management_year
        FOREIGN KEY (year)
        REFERENCES public.dim_year(year)
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT fk_management_position
        FOREIGN KEY (management_position_code)
        REFERENCES public.dim_management_position(
            management_position_code
        )
        ON UPDATE CASCADE
        ON DELETE RESTRICT,

    CONSTRAINT chk_women_management_percentage
        CHECK (
            women_management_percentage BETWEEN 0 AND 100
            OR women_management_percentage IS NULL
        )
);


-- =====================================================
-- 3. INITIAL REFERENCE DATA
-- =====================================================

-- Populate analysis years: 2015-2024

INSERT INTO public.dim_year (year)
SELECT generate_series(2015, 2024)::SMALLINT
ON CONFLICT (year) DO NOTHING;

COMMIT;

-- =====================================================
-- END OF SCHEMA
-- =====================================================
