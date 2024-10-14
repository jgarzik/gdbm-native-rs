FROM debian:12.5 AS debian-base
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends g++ ca-certificates


# 64bit LE
FROM debian-base AS testgen-x86_64
RUN apt-get install -y --no-install-recommends libgdbm-dev
WORKDIR /tmp
COPY src/testgen.cc .
RUN g++ -o testgen -static testgen.cc /usr/lib/x86_64-linux-gnu/diet/lib-x86_64/libgdbm.a


# 32bit LE
FROM debian-base AS testgen-i386
RUN dpkg --add-architecture i386 && \
    apt-get update && \
    apt-get install -y --no-install-recommends libgdbm-dev:i386 g++-multilib
WORKDIR /tmp
COPY src/testgen.cc .
RUN g++ -m32 -o testgen -static testgen.cc /usr/lib/i386-linux-gnu/diet/lib-i386/libgdbm.a


FROM debian-base AS libgdbm-sources
RUN apt-get install -y --no-install-recommends build-essential autoconf automake autopoint gettext libtool flex bison texinfo git g++
WORKDIR /tmp
RUN git clone https://git.savannah.gnu.org/git/gdbm.git
WORKDIR /tmp/gdbm
RUN autoreconf --install


# 32bit BE
FROM libgdbm-sources as testgen-mips
RUN apt-get install -y --no-install-recommends g++-mips-linux-gnu 
WORKDIR /tmp/gdbm
RUN ./configure --prefix /tmp/out --host=mips-linux-gnu && make && make install
WORKDIR /tmp
COPY src/testgen.cc .
RUN mips-linux-gnu-g++ -o testgen -I out/include -static testgen.cc out/lib/libgdbm.a


# 64bit BE
FROM libgdbm-sources as testgen-mips64
RUN apt-get install -y --no-install-recommends g++-mips64-linux-gnuabi64 
WORKDIR /tmp/gdbm
RUN ./configure --prefix /tmp/out --host=mips64-linux-gnuabi64 && make && make install
WORKDIR /tmp
COPY src/testgen.cc .
RUN mips64-linux-gnuabi64-g++ -o testgen -I out/include -static testgen.cc out/lib/libgdbm.a


FROM debian-base AS testdata
RUN apt-get install -y --no-install-recommends qemu-system-mips qemu-system-mips64
WORKDIR /tmp
COPY --from=testgen-x86_64 /tmp/testgen .
RUN ./testgen -o basic.db.le64 -j basic.json.le64 -p basic && \
    ./testgen -o empty.db.le64 -j empty.json.le64 -p empty && \
    ./testgen -n -o basic.db.le64.numsync -j /dev/null -p basic && \
    ./testgen -n -o empty.db.le64.numsync -j /dev/null -p empty
COPY --from=testgen-i386 /tmp/testgen .
RUN ./testgen -o basic.db.le32 -j basic.json.le32 -p basic && \
    ./testgen -o empty.db.le32 -j empty.json.le32 -p empty && \
    ./testgen -n -o basic.db.le32.numsync -j /dev/null -p basic && \
    ./testgen -n -o empty.db.le32.numsync -j /dev/null -p empty
COPY --from=testgen-mips64 /tmp/testgen .
RUN ./testgen -o basic.db.be64 -j basic.json.be64 -p basic && \
    ./testgen -o empty.db.be64 -j empty.json.be64 -p empty && \
    ./testgen -n -o basic.db.be64.numsync -j /dev/null -p basic && \
    ./testgen -n -o empty.db.be64.numsync -j /dev/null -p empty
COPY --from=testgen-mips /tmp/testgen .
RUN ./testgen -o basic.db.be32 -j basic.json.be32 -p basic && \
    ./testgen -o empty.db.be32 -j empty.json.be32 -p empty && \
    ./testgen -n -o basic.db.be32.numsync -j /dev/null -p basic && \
    ./testgen -n -o empty.db.be32.numsync -j /dev/null -p empty
RUN rm testgen
ENTRYPOINT ["sh", "-c", "cp /tmp/* /outdir"]
