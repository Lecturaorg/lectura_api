--DROP TABLE IF EXISTS wikiauthor_details, wikiauthor_externals, wikiauthor_labels, wikiauthor_texts;
CREATE TABLE wikiauthor_details (
	author varchar,
	authordesc varchar,
	"authorLabel" varchar,
	"akaLabel" varchar,
	"genderLabel" varchar,
	birthdate varchar,
	birthyear int,
	birthmonth int,
	birthday int,
	birthplace varchar,
	"birthplaceLabel" varchar,
	"birthplacecountryLabel" varchar,
	deathdate varchar,
	deathyear int,
	deathmonth int,
	deathday int,
	deathplace varchar,
	"deathplaceLabel" varchar,
	deathplacecountry varchar,
	"deathplacecountryLabel" varchar,
	floruit varchar,
	"occupationsLabel" varchar,
	"languagesLabel" varchar,
	"spouseLabel" varchar,
	"imageLabel" varchar,
	"nativenameLabel" varchar,
	"citizenshipLabel" varchar,
	article varchar
);
CREATE TABLE wikiauthor_externals (
	author varchar,
	"propertyLabel" varchar,
	value varchar
);
CREATE TABLE wikiauthor_labels (
	author varchar,
	"authorLabel" varchar,
	"authorLabelLang" varchar(255)
);
CREATE TABLE wikiauthor_texts (
	text_q varchar(255),
	author varchar,
	book varchar,
	"bookLabel" varchar,
	"akaLabel" varchar,
	bookdesc varchar,
	"titleLabel" varchar,
	"typeLabel" varchar,
	"formLabel" varchar,
	"genreLabel" varchar,
	image varchar,
	"publYear" int,
	publication varchar,
	"languageLabel" varchar,
	"origincountryLabel" varchar,
	"dopYear" int,
	inception varchar(255),
	"inceptionYear" int,
	"metreLabel" varchar(255),
	"publisherLabel" varchar,
	"lengthLabel" varchar(255)
);

CREATE TABLE wikitext_details (
	text_q varchar(255),
	author varchar(255),
	"bookLabel" varchar(1000),
	"authorLabel" varchar(1000),
	image varchar(1000),
	"akaLabel" varchar(255),
	bookdesc varchar(1000),
	"titleLabel" varchar(1000),
	"typeLabel" varchar(255),
	"formLabel" varchar(255),
	"genreLabel" varchar(255),
	"publYear" int,
	publication varchar(255),
	"languageLabel" varchar(255),
	"origincountryLabel" varchar(255),
	"dopYear" int,
	inception varchar(255),
	"inceptionYear" int,
	"metreLabel" varchar(255),
	"publisherLabel" varchar(255),
	"lengthLabel" int,
	"timeperiodLabel" varchar(255),
	"awardsLabel" varchar(255),
	"publishedInLabel" varchar(255),
	"copyrightLabel" varchar(255),
	article varchar(1000)
);

CREATE TABLE wikitext_externals (
	text_q varchar(255),
	"propertyLabel" varchar(1000),
	value varchar(1000)
);

CREATE TABLE wikitext_labels (
	text_q varchar(255),
	"textLabel" varchar(1000),
	"textLabelLang" varchar(255)
);
