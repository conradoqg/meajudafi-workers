CREATE TABLE fbcdata_sgs_11i (
    id UUID NOT NULL UNIQUE,
    DATA DATE NOT NULL,
    VALOR DOUBLE PRECISION,	
    PRIMARY KEY(id, DATA),
    CONSTRAINT fbcdata_sgs_11i_DATA UNIQUE (DATA)
);
CREATE INDEX fbcdata_sgs_11i_data_index
    ON fbcdata_sgs_11i USING btree
    (DATA ASC NULLS LAST);    

CREATE TABLE fbcdata_sgs_7i (
    id UUID NOT NULL UNIQUE,
    DATA DATE NOT NULL,
    VALOR DOUBLE PRECISION,	
    PRIMARY KEY(id, DATA),
    CONSTRAINT fbcdata_sgs_7i_DATA UNIQUE (DATA)
);
CREATE INDEX fbcdata_sgs_7i_data_index
    ON fbcdata_sgs_7i USING btree
    (DATA ASC NULLS LAST);    

CREATE TABLE fbcdata_sgs_433i (
    id UUID NOT NULL UNIQUE,
    DATA DATE NOT NULL,
    VALOR DOUBLE PRECISION,	
    PRIMARY KEY(id, DATA),
    CONSTRAINT fbcdata_sgs_433i_DATA UNIQUE (DATA)
);
CREATE INDEX fbcdata_sgs_433i_data_index
    ON fbcdata_sgs_433i USING btree
    (DATA ASC NULLS LAST);    