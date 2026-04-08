#include <pcap.h>

#include <cstdint>
#include <cstdio>
#include <iostream>
#include <iomanip>

static inline uint16_t be16(const uint8_t* p) {
    return (uint16_t(p[0]) << 8) | uint16_t(p[1]);
}

static inline uint64_t mac48_to_u64(const uint8_t* m) {
    return (uint64_t)m[0] << 40 |
           (uint64_t)m[1] << 32 |
           (uint64_t)m[2] << 24 |
           (uint64_t)m[3] << 16 |
           (uint64_t)m[4] << 8  |
           (uint64_t)m[5];
}

static inline bool is_vlan_ethertype(uint16_t ethertype) {
    return ethertype == 0x8100 || ethertype == 0x88A8 || ethertype == 0x9100;
}

struct L3Info {
    uint64_t dst_mac = 0;
    uint16_t vlan = 0;
    uint16_t ethertype = 0;
    size_t l3_offset = 0;
};

struct L4Info {
    uint16_t dst_port = 0;
};

static bool parse_ethernet_and_vlan(const uint8_t* data, size_t caplen, L3Info& out) {
    if (caplen < 14) {
        return false;
    }

    out.dst_mac = mac48_to_u64(data + 0);

    size_t off = 12;
    uint16_t ethertype = be16(data + off);
    off += 2;

    bool vlan_set = false;
    uint16_t vlan = 0;

    while (is_vlan_ethertype(ethertype)) {
        if (caplen < off + 4) {
            return false;
        }

        uint16_t tci = be16(data + off);
        uint16_t inner_ethertype = be16(data + off + 2);

        if (!vlan_set) {
            vlan = uint16_t(tci & 0x0FFF);
            vlan_set = true;
        }

        ethertype = inner_ethertype;
        off += 4;
    }

    out.vlan = vlan;
    out.ethertype = ethertype;
    out.l3_offset = off;
    return true;
}

static bool parse_ipv4_dst_port(const uint8_t* data, size_t caplen, size_t off, L4Info& out) {
    if (caplen < off + 20) {
        return false;
    }

    uint8_t ver_ihl = data[off];
    uint8_t version = ver_ihl >> 4;
    uint8_t ihl = ver_ihl & 0x0F;
    if (version != 4 || ihl < 5) {
        return false;
    }

    size_t ip_hlen = size_t(ihl) * 4;
    if (caplen < off + ip_hlen + 4) {
        return false;
    }

    uint8_t ip_proto = data[off + 9];
    if (ip_proto != 6 && ip_proto != 17) {
        return false;
    }

    const uint8_t* l4 = data + off + ip_hlen;
    out.dst_port = be16(l4 + 2);
    return true;
}

static bool parse_ipv6_dst_port(const uint8_t* data, size_t caplen, size_t off, L4Info& out) {
    if (caplen < off + 40) {
        return false;
    }

    uint8_t version = data[off] >> 4;
    if (version != 6) {
        return false;
    }

    uint8_t next_header = data[off + 6];
    size_t cur = off + 40;

    while (true) {
        if (next_header == 6 || next_header == 17) {
            if (caplen < cur + 4) {
                return false;
            }
            out.dst_port = be16(data + cur + 2);
            return true;
        }

        if (next_header == 0 || next_header == 43 || next_header == 60) {
            if (caplen < cur + 8) {
                return false;
            }
            uint8_t nh = data[cur + 0];
            uint8_t ext_len = data[cur + 1];
            size_t skip = (size_t(ext_len) + 1) * 8;
            if (caplen < cur + skip) {
                return false;
            }
            next_header = nh;
            cur += skip;
            continue;
        }

        if (next_header == 44) {
            if (caplen < cur + 8) {
                return false;
            }
            next_header = data[cur + 0];
            cur += 8;
            continue;
        }

        if (next_header == 51) {
            if (caplen < cur + 8) {
                return false;
            }
            uint8_t nh = data[cur + 0];
            uint8_t ext_len = data[cur + 1];
            size_t skip = (size_t(ext_len) + 2) * 4;
            if (caplen < cur + skip) {
                return false;
            }
            next_header = nh;
            cur += skip;
            continue;
        }

        return false;
    }
}

static bool extract_event(const uint8_t* data, size_t caplen,
                          uint64_t& ts_us, uint64_t& key, uint16_t& dst_port,
                          const timeval& ts) {
    L3Info l3;
    if (!parse_ethernet_and_vlan(data, caplen, l3)) {
        return false;
    }

    L4Info l4;
    bool ok = false;
    if (l3.ethertype == 0x0800) {
        ok = parse_ipv4_dst_port(data, caplen, l3.l3_offset, l4);
    } else if (l3.ethertype == 0x86DD) {
        ok = parse_ipv6_dst_port(data, caplen, l3.l3_offset, l4);
    }

    if (!ok) {
        return false;
    }

    ts_us = uint64_t(ts.tv_sec) * 1000000ULL + uint64_t(ts.tv_usec);
    key = (l3.dst_mac << 12) | uint64_t(l3.vlan & 0x0FFFu);
    dst_port = l4.dst_port;
    return true;
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

    const u_char* data = nullptr;
    struct pcap_pkthdr* hdr = nullptr;

    bool have_prev_ts = false;
    int64_t prev_sec = 0;
    int64_t prev_usec = 0;
    uint64_t nonmonotonic_count = 0;
    uint64_t emitted_events = 0;

    while (true) {
        int rc = pcap_next_ex(p, &hdr, &data);
        if (rc == -2) {
            break;
        }
        if (rc == -1) {
            std::cerr << "pcap_next_ex error: " << pcap_geterr(p) << "\n";
            pcap_close(p);
            return 3;
        }
        if (rc != 1) {
            continue;
        }

        int64_t cur_sec = hdr->ts.tv_sec;
        int64_t cur_usec = hdr->ts.tv_usec;
        if (have_prev_ts) {
            if (cur_sec < prev_sec || (cur_sec == prev_sec && cur_usec < prev_usec)) {
                ++nonmonotonic_count;
            }
        }
        prev_sec = cur_sec;
        prev_usec = cur_usec;
        have_prev_ts = true;

        uint64_t ts_us = 0;
        uint64_t key = 0;
        uint16_t dst_port = 0;
        if (!extract_event(data, hdr->caplen, ts_us, key, dst_port, hdr->ts)) {
            continue;
        }

        std::cout << ts_us << '\t' << key << '\t' << dst_port << '\n';
        ++emitted_events;
    }

    pcap_close(p);

    if (nonmonotonic_count > 0) {
        std::cerr << "WARNING: non-monotonic timestamps detected: "
                  << nonmonotonic_count << " packets\n";
    }
    std::cerr << "Emitted normalized events: " << emitted_events << "\n";
    return 0;
}