CREATE TABLE WIKI_AUTHORS (
	--id SERIAL PRIMARY KEY,
	author varchar(255),
	authorLabel varchar(255),
	birthdateLabel date,
	birthplaceLabel varchar(255),
	deathdateLabel date,
	deathplaceLabel varchar(255),
	floruitLabel varchar(255),
	occupationsLabel varchar(255),
	country varchar(255),
	dateuploaded timestamptz default now()
)
