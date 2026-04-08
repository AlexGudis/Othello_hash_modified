#include <pcap.h>
#include <cstdint>
#include <cstdio>
#include <unordered_map>
#include <iostream>
#include <iomanip>

static inline uint64_t mac48_to_u64(const uint8_t* m) {
    return (uint64_t)m[0] << 40 |
           (uint64_t)m[1] << 32 |
           (uint64_t)m[2] << 24 |
           (uint64_t)m[3] << 16 |
           (uint64_t)m[4] << 8  |
           (uint64_t)m[5];
}

static inline void print_mac12(uint64_t mac) {
    std::ios old(nullptr);
    old.copyfmt(std::cout);
    std::cout << std::hex << std::nouppercase << std::setfill('0')
              << std::setw(12) << (mac & 0xFFFFFFFFFFFFULL);
    std::cout.copyfmt(old);
}

int main() {
    std::ios::sync_with_stdio(false);

    char errbuf[PCAP_ERRBUF_SIZE];
    pcap_t* p = pcap_fopen_offline(stdin, errbuf);
    if (!p) {
        std::cerr << "pcap_fopen_offline(stdin) failed: " << errbuf << "\n";
        return 1;
    }

    int dlt = pcap_datalink(p);
    if (dlt != DLT_EN10MB) {
        std::cerr << "Unsupported datalink type: " << dlt << " (need Ethernet)\n";
        pcap_close(p);
        return 2;
    }

    std::unordered_map<uint64_t, uint32_t> first;
    first.reserve(1'000'000);

    const u_char* data;
    struct pcap_pkthdr* hdr;

    while (true) {
        int rc = pcap_next_ex(p, &hdr, &data);
        // std::cout << "aa" << std::endl;
        if (rc == 1) {
            if (hdr->caplen < 14) continue;

            uint32_t sec = (uint32_t)hdr->ts.tv_sec;
            uint64_t dst = mac48_to_u64(data + 0);
            uint64_t src = mac48_to_u64(data + 6);

            if (!first.contains(src)) first.emplace(src, sec);
            if (!first.contains(dst)) first.emplace(dst, sec);

        } else if (rc == -2) {
            break;
        } else if (rc == -1) {
            std::cerr << "pcap_next_ex error: " << pcap_geterr(p) << "\n";
            pcap_close(p);
            return 3;
        }
    }

    pcap_close(p);

    // output: mac \t first_sec (for THIS file)
    for (auto &kv : first) {
        print_mac12(kv.first);
        std::cout << "\t" << kv.second << "\n";
    }
    return 0;
}