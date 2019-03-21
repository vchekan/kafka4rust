cargo build && \
    sudo cp target/debug/libkafka_plugin.so /usr/lib/x86_64-linux-gnu/wireshark/plugins/2.6/epan/ && \
    wireshark-gtk -r ../../dump.dat
