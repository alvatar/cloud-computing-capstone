# Start ntpd
sudo apt-get install -y ntp
sudo service ntp start

# Install Java
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
sudo apt-get install -y oracle-java8-installer
