import gaps_online as go
import numpy as np
from tqdm import tqdm
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Searching for gaps in MTBMoni data packets to indicate MTB outages')
    parser.add_argument('--telemetry-dir', default='', help='A directory with telemetry binaries, as received from the telemetry stream')
    parser.add_argument('-s','--start-time', type=int, default=-1, help='The run start time, e.g. as taken from the elog')
    parser.add_argument('-e','--end-time',type=int, default=-1, help='The run end time, e.g. as taken from the elog')
    parser.add_argument('-w', '--window', type = int, help = 'the window for which missing MTBMoni packets would constitute an outage')

    args = parser.parse_args()

    files = go.io.get_telemetry_binaries(args.start_time, args.end_time, data_dir=args.telemetry_dir)

    packet_ts = []
    moni_gaps = []

    for f in tqdm(files, desc='Reading files..'):
        treader = go.io.TelemetryPacketReader(str(f))
        for packet in treader:
            gcu = packet.header.gcutime
            if int(packet.header.packet_type) == 92:
                tp = go.io.TofPacket()
                tp.from_bytestream(packet.payload, 0)
                if int(tp.packet_type) == 90:
                    packet_ts.append(gcu)


    packet_ts.sort()
    for i in range(1, len(packet_ts)):
        start = packet_ts[i-1]
        end = packet_ts[i]
        duration = end - start

        if duration >= args.window:
            moni_gaps.append((start, end, duration))


    print('--------------------------------------------------------------------------------------------')
    print('Detected ' + str(len(moni_gaps))+ ' MTB outages with lenth greater than '+ str(args.window) + ' seconds between ' + str(args.start_time) + ' to '+ str(args.end_time) + ' seconds')
    for gap in moni_gaps:
        print(f'from {gap[0]} to {gap[1]} with duration {gap[2]}')

    print('--------------------------------------------------------------------------------------------')


    print(packet_ts[1]) 


