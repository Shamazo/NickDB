# declare parent image

FROM ubuntu:18.04
ARG DEBIAN_FRONTEND=noninteractive

# specify working directory inside the new docker container, creating it, if does not exist
WORKDIR /db

# NOTE: This Run Statement is Staff Only for Setup. Students should not modify this part.
#   We keep these commands together for "caching" within docker layers.
# What are all these dependencies?
#   build-essential is needed for Make
#   gcc is needed for compilation of C
#   sse4.2-support for SIMD support
#   psmisc convenience utilities for managing processes (e.g. killall processes by name)
#   valgrind for memory issue debugging
#   tmux for multiplexing between multiple windows within your interactive docker shell
#   python 2.7.x
#   strace for stack profiling & debugging
#   mutt for email formatting, this is required on our staff automated tests
#       emailing you summaries when your trial runs are done
RUN bash -c 'apt-get update && apt-get install -y \
    apt-utils \
    build-essential \
    gcc \
    sse4.2-support \
    psmisc \
    python \
    python3-pip \
    tmux \
    valgrind \
    strace \
    mutt \
    cmake \
    vim '


    
#   python stat packages: scipy, pandas. dependencies for test generation
RUN bash -c 'pip3 install scipy pandas'

#   linux tools are for utilities such as `perf` for counters / performance measurement
# enable linux-tools once MacOS linuxkit instruction support is patched and released
RUN bash -c 'apt-get install -y linux-tools-common linux-tools-generic && \
    # symlink proper linux tools to the perf shortcut \
    cd /usr/lib/linux-tools && \
    cd `ls -1 | head -n1` && \
    rm -f /usr/bin/perf && \
    ln -s `pwd`/perf /usr/bin/perf'


###################### Begin Customization ########################

####  NOTE: IF NEEDED, ADD ADDITIONAL IMAGE SETUP NEEDS FOR YOUR DEV REASONS HERE ####
# e.g. install your favorite dev tools:
# for more on Docker 'layering' for space amplification control:
#   https://stackoverflow.com/questions/39223249/

# RUN apt-get install emacs


# define any environmental variables you need?
# ENV MYLABEL helloworld

###################### End Customization ##########################

# start by cleaning and making, and then starting a shell with tmux
# tmux is a nice window multiplexer / manager within a single terminal
# for a quick rundown of tmux window management shortcuts:
#   https://tmuxcheatsheet.com/
CMD cd src
CMD cmake . && make all && /bin/bash && tmux
