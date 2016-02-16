#!/usr/bin/env sh

# Install custom development tools
sudo apt-get update
sudo apt-get install -y vim emacs mosh git tmux zsh unzip

# Install JDK 8                                                                 
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get update
sudo apt-get install -y oracle-java8-installer

# Start ntpd
sudo apt-get install -y ntp
sudo service ntp start

# Install Ambari
cd /etc/apt/sources.list.d
sudo wget http://public-repo-1.hortonworks.com/ambari/ubuntu14/2.x/updates/2.1.2/ambari.list
sudo apt-key adv --recv-keys --keyserver keyserver.ubuntu.com B9733A7A07513CAD
sudo apt-get update
sudo apt-get install -y ambari-server

# Setup Ambari
sudo ambari-server setup
sudo ambari-server start

# Oh my zsh
curl -L http://install.ohmyz.sh | sed -n '/chsh/!p' | sh
sudo chsh -s $(which zsh) ubuntu

