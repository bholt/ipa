#######################################################
# Extension of basic dev image for Scala development.
#######################################################
FROM bholt/cassandra
MAINTAINER Brandon Holt <holt.bg@gmail.com>

RUN apt-get install -y zsh vim locales

# Set up nice zsh environment
RUN git clone http://github.com/bholt/vm.git &&\
    sh /vm/base/oh-my-zsh-install.sh || true &&\
    cat /vm/base/profile.bash >> /root/.bashrc; \
    cat /vm/base/profile.zsh > /root/.zshrc; \
    cat /vm/base/gitconfig >> /root/.gitconfig; \
    rm -r /root/.oh-my-zsh/custom; \
    cp -r /vm/base/oh-my-zsh-custom /root/.oh-my-zsh/custom; \
    cp /vm/base/dircolorsrc /root/.dircolorsrc; \
    echo '🐳 ' > /root/.box-name &&\
    rm -r /vm &&\
    chsh -s /usr/bin/zsh root

ENV SHELL /usr/bin/zsh


# So we can use unicode (and emoji!)
RUN locale-gen en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US:en

RUN echo 'up(){ pushd /src >/dev/null; rsync -a /sync/owl . --exclude=target/ --exclude=.idea/; popd >/dev/null; };\n' >> ~/.zshrc

CMD ["/usr/bin/zsh"]
