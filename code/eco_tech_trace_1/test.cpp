#include <pcap.h>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <unordered_set>
#include <iostream>

static inline uint64_t mac48_to_u64(const uint8_t* m) {
    return (uint64_t)m[0] << 40 |
           (uint64_t)m[1] << 32 |
           (uint64_t)m[2] << 24 |
           (uint64_t)m[3] << 16 |
           (uint64_t)m[4] << 8  |
           (uint64_t)m[5];
}

int main(int argc, char** argv) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " capture.pcap\n";
        return 1;
    }

    char errbuf[PCAP_ERRBUF_SIZE];
    pcap_t* p = pcap_open_offline(argv[1], errbuf);
    if (!p) {
        std::cerr << "pcap_open_offline failed: " << errbuf << "\n";
        return 1;
    }

    int dlt = pcap_datalink(p);
    if (dlt != DLT_EN10MB) {
        std::cerr << "Unsupported datalink type: " << dlt
                  << " (expected DLT_EN10MB/Ethernet)\n";
        pcap_close(p);
        return 2;
    }

    std::unordered_set<uint64_t> seen;
    seen.reserve(5'000'000); // подстрой под ожидаемое число MAC

    const u_char* data;
    struct pcap_pkthdr* hdr;

    bool have_sec = false;
    uint32_t current_sec = 0;
    uint32_t new_in_sec = 0;

    uint64_t pkt_count = 0;

    auto flush_sec = [&](uint32_t sec) {
        std::cout << sec << "\t" << new_in_sec
                      << "\t" << seen.size()
                      << "\t" << pkt_count
                      << "\n";
            std::cout.flush(); // важно для "живого" вывода
        new_in_sec = 0;
    };

    while (true) {
        int rc = pcap_next_ex(p, &hdr, &data);
        if (rc == 1) {
            pkt_count++;

            if (hdr->caplen < 14) continue;

            uint32_t sec = (uint32_t)hdr->ts.tv_sec;

            if (!have_sec) {
                have_sec = true;
                current_sec = sec;
            } else if (sec != current_sec) {
                // выводим итог по предыдущей секунде
                flush_sec(current_sec);
                current_sec = sec;
            }

            uint64_t dst = mac48_to_u64(data + 0);
            uint64_t src = mac48_to_u64(data + 6);

            if (seen.insert(src).second) new_in_sec++;
            if (seen.insert(dst).second) new_in_sec++;

        } else if (rc == -2) { // EOF
            if (have_sec) flush_sec(current_sec);
            break;
        } else if (rc == -1) {
            std::cerr << "pcap_next_ex error: " << pcap_geterr(p) << "\n";
            pcap_close(p);
            return 3;
        }
    }

    pcap_close(p);
    return 0;
}