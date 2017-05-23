sudo npm install -g shapefile
wget http://www.naturalearthdata.com/http//www.naturalearthdata.com/download/110m/cultural/ne_110m_admin_0_countries.zip
unzip ne_110m_admin_0_countries.zip
shp2json ne_110m_admin_0_countries.shp -o world.json
sudo npm install -g d3-geo-projection
