--DROP TABLE IF EXISTS wikiauthor_details, wikiauthor_externals, wikiauthor_labels, wikiauthor_texts;
CREATE TABLE wikiauthor_details (
	author varchar(255),
	authordesc varchar(255),
	"authorLabel" varchar(255),
	"akaLabel" varchar(255),
	"genderLabel" varchar(255),
	birthdate varchar(255),
	birthyear int,
	birthmonth int,
	birthday int,
	birthplace varchar(255),
	"birthplaceLabel" varchar(255),
	"birthplacecountryLabel" varchar(255),
	deathdate varchar(255),
	deathyear int,
	deathmonth int,
	deathday int,
	deathplace varchar(255),
	"deathplaceLabel" varchar(255),
	deathplacecountry varchar(255),
	"deathplacecountryLabel" varchar(255),
	floruit varchar(255),
	"occupationsLabel" varchar(255),
	"languagesLabel" varchar(255),
	"spouseLabel" varchar(255),
	"imageLabel" varchar(255),
	"nativenameLabel" varchar(255),
	"citizenshipLabel" varchar(255),
	article varchar(255)
);
CREATE TABLE wikiauthor_externals (
	author varchar(255),
	"propertyLabel" varchar(255),
	value varchar(255)
);
CREATE TABLE wikiauthor_labels (
	author varchar(255),
	"authorLabel" varchar(255),
	"authorLabelLang" varchar(255)
);
CREATE TABLE wikiauthor_texts (
	text_q varchar(255),
	author varchar(255),
	book varchar(255),
	"bookLabel" varchar(255),
	"akaLabel" varchar(255),
	bookdesc varchar(255),
	"titleLabel" varchar(255),
	"typeLabel" varchar(255),
	"formLabel" varchar(255),
	"genreLabel" varchar(255),
	image varchar(255),
	"publYear" int,
	publication varchar(255),
	"languageLabel" varchar(255),
	"origincountryLabel" varchar(255),
	"dopYear" int,
	inception varchar(255),
	"inceptionYear" int,
	"metreLabel" varchar(255),
	"publisherLabel" varchar(255),
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
