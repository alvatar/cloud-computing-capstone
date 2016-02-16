#!/usr/bin/env bash

TARGET=hadoop@$1
TARGET_HOME=$TARGET:/home/hadoop
CREDS=$HOME/.ssh/coursera.pem

# Tools
ssh -i $CREDS $TARGET 'sudo yum -y install tmux zsh emacs vim git'
scp -i $CREDS ~/.emacs ~/.vimrc ~/.tmux.conf $TARGET_HOME

# Cassandra
ssh -i $CREDS $TARGET "sudo bash -c 'cat <<HERE > /etc/yum.repos.d/datastax.repo
[datastax]
name= DataStax Repo for Apache Cassandra
baseurl=http://rpm.datastax.com/community
enabled=1
gpgcheck=0
HERE'"
ssh -i $CREDS $TARGET 'sudo yum -y install cassandra'
ssh -i $CREDS $TARGET 'sudo pip install cassandra-driver'

# Shell
ssh -i $CREDS $TARGET 'sh -c "$(wget https://raw.github.com/robbyrussell/oh-my-zsh/master/tools/install.sh -O -)"'
ssh -i $CREDS $TARGET 'sudo chsh -s $(which zsh) hadoop'

# Code
ssh -i $CREDS $TARGET 'git clone https://github.com/alvatar/cloud-computing-capstone.git'
ssh -i $CREDS $TARGET 'cd ~/cloud-computing-capstone && git config --local github.user alvatar; git config --local user.email alvcastro@yahoo.es ; git config --local user.name "Álvaro Castro-Castilla"'
