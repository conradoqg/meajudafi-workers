CREATE EXTENSION ltree;

CREATE UNLOGGED TABLE public.progress (
	"path" ltree NOT NULL,
	"data" jsonb NULL,
	CONSTRAINT progress_pkey PRIMARY KEY ("path")
);
CREATE INDEX path_gist_idx ON public.progress ("path");
CREATE INDEX path_idx ON public.progress ("path");