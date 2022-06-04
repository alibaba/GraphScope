import os

from graphscope.framework.loader import Loader

def load_chinavis_dataset(sess, prefix="/home/graphscope/.gshttpserver/dataset/vis", directed=True):
    prefix = os.path.expandvars(prefix)

    vertices = {
        "Domain": (
            Loader(
                os.path.join(prefix, "Domain.csv"),
                header_row=True,
                delimiter=","
            ),
            None,
            "id"
        ),
        "ASN": (
            Loader(
                os.path.join(prefix, "ASN.csv"),
                header_row=True,
                delimiter=","
            ),
            None,
            "id"
        ),
        "Cert": (
            Loader(
                os.path.join(prefix, "Cert.csv"),
                header_row=True,
                delimiter=","
            ),
            None,
            "id"
        ),
        "IP_C": (
            Loader(
                os.path.join(prefix, "IP_C.csv"),
                header_row=True,
                delimiter=","
            ),
            None,
            "id"
        ),
        "IP": (
            Loader(
                os.path.join(prefix, "IP.csv"),
                header_row=True,
                delimiter=","
            ),
            None,
            "id"
        ),
        "Whois_Email": (
            Loader(
                os.path.join(prefix, "Whois_Email.csv"),
                header_row=True,
                delimiter=","
            ),
            None,
            "id"
        ),
        "Whois_Name": (
            Loader(
                os.path.join(prefix, "Whois_Name.csv"),
                header_row=True,
                delimiter=","
            ),
            None,
            "id"
        ),
        "Whois_Phone": (
            Loader(
                os.path.join(prefix, "Whois_Phone.csv"),
                header_row=True,
                delimiter=","
            ),
            None,
            "id"
        ),
    }

    edges = {
        "r_whois_name": [
            (
                Loader(
                    os.path.join(prefix, "r_whois_name_Domain_Domain.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Domain")
            ),
            (
                Loader(
                    os.path.join(prefix, "r_whois_name_Domain_Whois_Name.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Whois_Name")
            ),
            (
                Loader(
                    os.path.join(prefix, "r_whois_name_Domain_Whois_Phone.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Whois_Phone")
            ),
            (
                Loader(
                    os.path.join(prefix, "r_whois_name_Whois_Phone_Domain.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Whois_Phone"),
                ("dst_id", "Domain")
            ),
        ],
        "r_whois_phone": [
            (
                Loader(
                    os.path.join(prefix, "r_whois_phone_Whois_Name_Domain.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Whois_Name"),
                ("dst_id", "Domain")
            ),
            (
                Loader(
                    os.path.join(prefix, "r_whois_phone_Domain_Whois_Phone.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Whois_Phone")
            ),
        ],
        "r_asn": [
            (
                Loader(
                    os.path.join(prefix, "r_asn_IP_ASN.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "IP"),
                ("dst_id", "ASN")
            ),
        ],
        "r_cert": [
            (
                Loader(
                    os.path.join(prefix, "r_cert_Domain_Cert.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Cert")
            ),
        ],
        "r_cert_chain": [
            (
                Loader(
                    os.path.join(prefix, "r_cert_chain_Cert_Cert.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Cert"),
                ("dst_id", "Cert")
            ),
        ],
        "r_cidr": [
            (
                Loader(
                    os.path.join(prefix, "r_cidr_IP_IP_CIDR.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "IP"),
                ("dst_id", "IP_C")
            ),
        ],
        "r_cname": [
            (
                Loader(
                    os.path.join(prefix, "r_cname_Domain_Domain.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Domain")
            ),
        ],
        "r_dns_a": [
            (
                Loader(
                    os.path.join(prefix, "r_dns_a_Domain_IP.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "IP")
            ),
        ],
        "r_request_jump": [
            (
                Loader(
                    os.path.join(prefix, "r_request_jump_Domain_Domain.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Domain")
            ),
        ],
        "r_subdomain": [
            (
                Loader(
                    os.path.join(prefix, "r_subdomain_Domain_Domain.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Domain")
            ),
        ],
        "r_whois_email": [
            (
                Loader(
                    os.path.join(prefix, "r_whois_email_Domain_Whois_Email.csv"),
                    header_row=True,
                    delimiter=","
                ),
                None,
                ("src_id", "Domain"),
                ("dst_id", "Whois_Email")
            ),
        ],
    }

    # graph = sess.g(oid_type="string", directed=directed)
    # graph = graph.add_vertices(
        # os.path.join(prefix, "Domain.csv"), "Domain"
    # ).add_vertices(
        # os.path.join(prefix, "ASN.csv"), "ASN"
    # ).add_vertices(
        # os.path.join(prefix, "Cert.csv"), "Cert"
    # ).add_vertices(
        # os.path.join(prefix, "IP_C.csv"), "IP_C"
    # ).add_vertices(
        # os.path.join(prefix, "IP.csv"), "IP"
    # ).add_vertices(
        # os.path.join(prefix, "Whois_Email.csv"), "Whois_Email"
    # ).add_vertices(
        # os.path.join(prefix, "Whois_Name.csv"), "Whois_Name"
    # ).add_vertices(
        # os.path.join(prefix, "Whois_Phone.csv"), "Whois_Phone"
    # # ).add_edges(
        # # os.path.join(prefix, "r_whois_phone.csv"),
        # # "r_whois_phone",
        # # src_label="Domain",
        # # dst_label="Whois_Phone",
        # # src_field=1,
        # # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_whois_name.csv"),
        # "r_whois_name",
        # src_label="Domain",
        # dst_label="Whois_Name",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_whois_email.csv"),
        # "r_whois_email",
        # src_label="Domain",
        # dst_label="Whois_Email",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_subdomain.csv"),
        # "r_subdomain",
        # src_label="Domain",
        # dst_label="Domain",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_request_jump.csv"),
        # "r_request_jump",
        # src_label="Domain",
        # dst_label="Domain",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_dns_a.csv"),
        # "r_dns_a",
        # src_label="Domain",
        # dst_label="IP",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_cname.csv"),
        # "r_cname",
        # src_label="Domain",
        # dst_label="Domain",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_cidr.csv"),
        # "r_cidr",
        # src_label="IP",
        # dst_label="IP_C",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_cert.csv"),
        # "r_cert",
        # src_label="Domain",
        # dst_label="Cert",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_cert_chain.csv"),
        # "r_cert_chain",
        # src_label="Cert",
        # dst_label="Cert",
        # src_field=1,
        # dst_field=2,
    # ).add_edges(
        # os.path.join(prefix, "r_asn.csv"),
        # "r_asn",
        # src_label="IP",
        # dst_label="ASN",
        # src_field=1,
        # dst_field=2,
    # )

    return sess.load_from(edges, vertices, directed=directed, oid_type="string", generate_eid=True)
