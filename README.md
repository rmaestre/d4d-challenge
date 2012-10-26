# Motivation 
Such a beautiful project: a cross line between technology and human development. An engineer's approach to help people to manage better his resources and create a better world.

We are going to try to correlate micro-migrations between work-hours (we should estimate this window time) and movil and fix phone calls in order to manage roads projection more efficiently.

# Deadlines
* October 31, 2012: Deadline for registration and submission  of applications. Orange and the Chairman of the Committee evaluate the candidate  applications on a regular basis. Orange's anonymous datasets are made accessible  to the selected teams.
* January 31, 2013: Deadline for submission of  project.

# Project homepage
* [D4D Challenge](http://www.d4d.orange.com/home) 
* [Paper. Data for Development: the D4D Challenge on Mobile Phone Data](http://arxiv.org/abs/1210.0137) 

# Final abstract ?
bla bla ....

# SIG Free software
Download uDig to manage geographical layers from [http://udig.refractions.net/](http://udig.refractions.net/)

# CartoDB (not free!!)

Maps creation and visualization tool. It's possible to import SHP zipped files into tables for building maps. More info available at [CartoDB](http://cartodb.com/) and pricing [here](http://cartodb.com/pricing)


# Geographical layers (vectors)
It is available into "geo_layers" folder the next datasets:

* civ_gc_adg.zip: Land Cover (download from http://www.fao.org/geonetwork/srv/en/metadata.show?currTab=simple&id=37177)
* rails.zip: Lines with Rails lines (download from http://cod.humanitarianresponse.info/es/country-region/c%C3%B4te-divoire)
* route.zip: Road route lines (download from http://cod.humanitarianresponse.info/es/country-region/c%C3%B4te-divoire)
* CDI-level_1_SHP.zip: Regions (9) - Latitude & Longitude Coordinates (downloaded from http://www.maplibrary.org/stacks/africa/Cote%20d%60Ivoire/index.php)
* CDI_admin_SHP.zip: Departments (59) - Latitude & Longitude Coordinates (downloaded from http://www.maplibrary.org/stacks/africa/Cote%20d%60Ivoire/index.php)
* {59} Xxx_SHP.zip: A map for each department, enumerating their inner sub-prefectures (230, TBC with D4D datasets) (downloaded from http://www.maplibrary.org/stacks/africa/Cote%20d%60Ivoire/index.php)
* CIV_wat.zip: Rivers, canals, and lakes. Seperate files for line and area features (download from http://www.diva-gis.org/gData)	

# First step with uDig

This step should be performed for each geographical resource

* Open uDig.app
* File > New Project (Choose the name to save your project)
* Layer > Add > Select "Files" > Select your file with .shp extension (e.g.: RESEAU_ROUTIER.shp)
	
![uDig first load](https://raw.github.com/rmaestre/d4d-challenge/master/doc/img/udig_first_load.png?login=rmaestre&token=3a881e97db2dbfd3f32585885c67501b)

# Do not forget
... to enjoy this project  :) !!!