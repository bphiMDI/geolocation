import binascii, requests, json, pandas as pd

##old code

def decode_cgi(cgi:list):
    if len(cgi) > 0:
        group_list = []
        final_list = []
        for cell_id in cgi:
            data = {}
            data_len = len(cell_id)
            octets = [cell_id[i:i+4] for i in range(0, data_len, 4)]
            for n in range(len(octets)):
                if n < 2:
                    octet_binary = bin(int(octets[n], 16))[2:].zfill(16)
                    octet_bins = [octet_binary[i:i+4] for i in range(0, 16, 4)]
                    octet_dec = [int(i, 2) for i in octet_bins]
                else:
                    octet_dec = int(octets[n], 16)
                data[n+1] = octet_dec
            group_list.append(data)
        for group in group_list:
            cgi_dict = {}
            cgi_dict['mcc'] = ''.join(str(i) for i in [group[1][3], group[1][2], group[2][1]])
            cgi_dict['mnc'] = ''.join(str(i) for i in [group[2][3], group[2][2]])
            cgi_dict['lac'] = group[3]
            cgi_dict['cell_id'] = group[4]
            final_list.append(cgi_dict)
        return final_list

def request_from_google(data:list):
    if len(data) > 0:
        pass

def decode_cgi_old(data: list):
    if len(data) > 0:
        full_list = []
        for uli in data:
            location = {}
            binary_string = binascii.unhexlify(uli)
            mcc_hex = uli[3] + uli[2] + uli[5]
            location['mcc'] = int(mcc_hex)
            mnc_hex = uli[7] + uli[6]
            location['mnc'] = int(mnc_hex)

            first_byte = binary_string[0]
            if first_byte == 130:
                cid_hex = uli[19:]
                location['lac'] = None
                location['radioType'] = "lte"
            elif first_byte == 1 or len(uli) == 16:
                lac_hex = uli[8:12]
                location['lac'] = int(lac_hex, base=16)
                cid_hex = uli[12:]
                location['radioType'] = 'gsm'
            location['cid'] = int(cid_hex, base=16)
            full_list.append(location)
        return full_list


def from_csv_old(path, delimiter=None, *args):
    header = ['type', 'mcc', 'mnc', 'cgi', 'country']
    df = pd.read_csv(path, names=header, delimiter=delimiter, *args)
    cgi_list = df['cgi'].tolist()
    return cgi_list[1:]





if __name__ == '__main__':
    list1 = ['8202F85109C602F8510651D23E', '0032f4030921b8e8', '0102f810630e3939', '0002f810248619f7']
    print(decode_cgi(list1))









