CREATE TABLE fbcdata_sgs_1i (
    id UUID NOT NULL UNIQUE,
    DATA DATE NOT NULL,
    VALOR DOUBLE PRECISION,	
    PRIMARY KEY(id, DATA),
    CONSTRAINT fbcdata_sgs_1i_DATA UNIQUE (DATA)
);
CREATE INDEX fbcdata_sgs_1i_data_index
    ON fbcdata_sgs_1i USING btree
    (DATA ASC NULLS LAST);    

CREATE TABLE fbcdata_sgs_21619i (
    id UUID NOT NULL UNIQUE,
    DATA DATE NOT NULL,
    VALOR DOUBLE PRECISION,	
    PRIMARY KEY(id, DATA),
    CONSTRAINT fbcdata_sgs_21619i_DATA UNIQUE (DATA)
);
CREATE INDEX fbcdata_sgs_21619i_data_index
    ON fbcdata_sgs_21619i USING btree
    (DATA ASC NULLS LAST);    