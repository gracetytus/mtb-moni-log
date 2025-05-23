import argparse
import gc
from datetime import datetime, timezone as UTC
import matplotlib.pyplot as plt
from tqdm import tqdm
import go.io
import go.tof.monitoring

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Detect MTB crashes by comparing rate and lost_rate in MTBMoniData')
    parser.add_argument('--telemetry-dir', default='', help='A directory with telemetry binaries, as received from the telemetry stream')
    parser.add_argument('-s','--start-time', type=int, default=-1, help='The run start time, e.g. as taken from the elog')
    parser.add_argument('-e','--end-time',type=int, default=-1, help='The run end time, e.g. as taken from the elog')

    args = parser.parse_args()

    files = go.io.get_telemetry_binaries(args.start_time, args.end_time, data_dir=args.telemetry_dir)
    packet_ts = []

    for f in tqdm(files, desc='Reading files..'):
        treader = go.io.TelemetryPacketReader(str(f))
        for packet in treader:
            gcu = packet.header.gcutime
            if int(packet.header.packet_type) == 92:
                tp = go.io.TofPacket()
                tp.from_bytestream(packet.payload, 0)
                if int(tp.packet_type) == 90:
                    moni = go.tof.monitoring.MtbMoniData()
                    moni.from_tofpacket(tp)
                    packet_ts.append((gcu, moni.tiu_busy_len, moni.daq_queue_len, moni.fpga_temp,
                                      moni.rate, moni.lost_rate, moni.vccaux, moni.vccbram, moni.vccint))

    packet_ts.sort()
    moni_gaps = []

    gap_open = False
    gap_start = None
    gap_end = None

    for pkt in packet_ts:
        timestamp, tiu, daq, temp, rate, lost_rate, vaux, vbram, vint = pkt
        is_gap = (lost_rate > rate) or (rate == 0 and lost_rate == 0)

        if is_gap:
            if not gap_open:
                gap_start = timestamp
                gap_info = (tiu, daq, temp, rate, lost_rate, vaux, vbram, vint)
                gap_open = True
            gap_end = timestamp
        else:
            if gap_open:
                duration = gap_end - gap_start
                moni_gaps.append((gap_start, gap_end, duration, *gap_info))
                gap_open = False

    if gap_open:
        duration = gap_end - gap_start
        moni_gaps.append((gap_start, gap_end, duration, *gap_info))

    dt_start = datetime.fromtimestamp(args.start_time, UTC)
    dt_end = datetime.fromtimestamp(args.end_time, UTC)
    dt1 = datetime.fromtimestamp(packet_ts[0][0], UTC)
    dt2 = datetime.fromtimestamp(packet_ts[-1][0], UTC)

    packet_ts.clear()
    gc.collect()

    for i in range(len(moni_gaps)):
        t_start = moni_gaps[i][0]
        new_bins = go.io.get_telemetry_binaries((t_start - 300), t_start, data_dir=args.telemetry_dir)

        evt_hb_list = []
        mtb_hb_list = []

        for binary in new_bins:
            treader = go.io.TelemetryPacketReader(str(binary))
            for p in treader:
                if int(p.header.packet_type) == 92:
                    tp_hb = go.io.TofPacket()
                    tp_hb.from_bytestream(p.payload, 0)

                    if int(tp_hb.packet_type) == 62:
                        mtb_hb = go.tof.monitoring.MTBHeartbeat()
                        mtb_hb.from_tofpacket(tp_hb)
                        mtb_hb_list.append(mtb_hb)

                    if int(tp_hb.packet_type) == 63:
                        evt_hb = go.tof.monitoring.EVTBLDRHeartbeat()
                        evt_hb.from_tofpacket(tp_hb)
                        evt_hb_list.append(evt_hb)

        mtb_data = (None, None, None, None, None)
        evt_data = (None, None, None, None, None, None)

        if mtb_hb_list:
            mtb_hb = mtb_hb_list[-1]
            mtb_data = (mtb_hb.total_elapsed, mtb_hb.n_events, mtb_hb.evq_num_events_last,
                        mtb_hb.n_ev_unsent, mtb_hb.n_ev_missed)

        if evt_hb_list:
            evt_hb = evt_hb_list[-1]
            evt_data = (evt_hb.n_mte_received_tot, evt_hb.n_rbe_received_tot,
                        evt_hb.n_mte_skipped, evt_hb.n_timed_out,
                        evt_hb.event_cache_size, evt_hb.event_cache_size)

        moni_gaps[i] = (*moni_gaps[i], mtb_data, evt_data)

    output_file = f'/home/gtytus/mtb-moni-log/reports/12Dec/{args.start_time}_{args.end_time}_mtb_outages_report.txt'

    with open(output_file, 'w') as f:
        f.write(f'the start time was {dt_start.strftime("%Y-%m-%d %H:%M:%S UTC")} and the first MTBMoniData was received {dt1.strftime("%Y-%m-%d %H:%M:%S UTC")}\n')
        f.write(f'the end time was {dt_end.strftime("%Y-%m-%d %H:%M:%S UTC")} and the last MTBMoniData was received {dt2.strftime("%Y-%m-%d %H:%M:%S UTC")}\n')
        f.write('--------------------------------------------------------------------------------------------\n')
        f.write(f'Detected {len(moni_gaps)} MTB outages between {args.start_time} to {args.end_time} seconds\n\n')
        for gap in moni_gaps:
            f.write(f'from {gap[0]} to {gap[1]} with duration {gap[2]}\n')
            f.write(f'---TIU busy: {gap[3]}, DAQ queue: {gap[4]}, Temp: {gap[5]}, Rate: {gap[6]}, Lost Rate: {gap[7]}\n')
            f.write(f'---VCC aux: {gap[8]}, VCC bram: {gap[9]}, VCC int: {gap[10]}\n')
            f.write(f'---MTB: elapsed {gap[11][0]}, events {gap[11][1]}, q-size {gap[11][2]}, unsent {gap[11][3]}, missed {gap[11][4]}\n')
            f.write(f'---EVT: MTE {gap[12][0]}, RBE {gap[12][1]}, skipped {gap[12][2]}, timed out {gap[12][3]}, cache size {gap[12][4]}, ID cache size {gap[12][5]}\n')
            f.write('-------------------------------------------------------------------------------------------\n')

    print(f'Detected {len(moni_gaps)} MTB outages between {args.start_time} to {args.end_time}')
    print(f'Additional output written to {output_file}')
