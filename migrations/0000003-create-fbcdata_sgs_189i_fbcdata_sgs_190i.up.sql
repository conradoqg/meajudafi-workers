CREATE TABLE fbcdata_sgs_189i (
    id UUID NOT NULL UNIQUE,
    DATA DATE NOT NULL,
    VALOR DOUBLE PRECISION,	
    PRIMARY KEY(id, DATA),
    CONSTRAINT fbcdata_sgs_189i_DATA UNIQUE (DATA)
);
CREATE INDEX fbcdata_sgs_189i_data_index
    ON fbcdata_sgs_189i USING btree
    (DATA ASC NULLS LAST);    

CREATE TABLE fbcdata_sgs_190i (
    id UUID NOT NULL UNIQUE,
    DATA DATE NOT NULL,
    VALOR DOUBLE PRECISION,	
    PRIMARY KEY(id, DATA),
    CONSTRAINT fbcdata_sgs_190i_DATA UNIQUE (DATA)
);
CREATE INDEX fbcdata_sgs_190i_data_index
    ON fbcdata_sgs_190i USING btree
    (DATA ASC NULLS LAST);    