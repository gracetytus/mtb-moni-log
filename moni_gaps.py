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

                    moni = go.tof.monitoring.MtbMoniData()
                    moni.from_tofpacket(tp)
                    tiu_busy = moni.tiu_busy_len
                    daq_queue = moni.daq_queue_len
                    temp = moni.fpga_temp
                    rate = moni.rate
                    lost_rate = moni.lost_rate
                    #vccint = moni.vccint
                    #vccbram = moni.vccbram
                    #vccaux = moni.vccaux

                    mtb_hb = go.tof.monitoring.MTBHeartbeat()
                    mtb_hb.from_tofpacket(packet)
                    total_elapsed = mtb_hb.total_elapsed
                    n_events = mtb_hb.n_events
                    evq_n_last = mtb_hb.evq_num_events_last
                    n_ev_unsent = mtb_hb.n_ev_unsent
                    n_ev_missed = mtb_hb.n_ev_missed

                    evt_hb = go.tof.monitoring.EVTBLDRHeartbeat()
                    evt_hb.from_tofpacket(packet)
                    n_mte_received = evt_hb.n_mte_received_tot
                    n_rbe_received = evt_hb.n_rbe_received_tot
                    n_mte_skipped = evt_hb.n_mte_skipped
                    n_timed_out = evt_hb.n_timed_out
                    cache_size = evt_hb.event_cache_size
                    evt_id_cache_size = evt_hb.event_cache_size



                    packet_ts.append((gcu, tiu_busy, daq_queue, temp, rate, lost_rate, total_elapsed, n_events, evq_n_last, n_ev_unsent, n_ev_missed, n_mte_received, n_rbe_received, n_mte_skipped, n_timed_out, cache_size, evt_id_cache_size))


    packet_ts.sort()
    for i in range(1, len(packet_ts)):
        start = packet_ts[i-1][0]
        end = packet_ts[i][0]
        duration = end - start

        tiu = packet_ts[i-1][1]
        daq = packet_ts[i-1][2]
        t = packet_ts[i-1][3]
        r = packet_ts[i-1][4]
        lr = packet_ts[i-1][5]
        #vint = packet_ts[i-1][6]
        #vbram = packet_ts[i-1][7]
        #vaux = packet_ts[i-1][8]

        time_elapsed = packet_ts[i-1][6]
        nevents = packet_ts[i-1][7]
        evq = packet_ts[i-1][8]
        ev_unsent = packet_ts[i-1][9]
        ev_missed = packet_ts[i-1][10]
        mte_rec = packet_ts[i-1][11]
        rbe_rec = packet_ts[i-1][12]
        mte_skipped = packet_ts[i-1][13]
        time_out = packet_ts[i-1][14]
        cache = packet_ts[i-1][15]
        evt_id_cache = packet_ts[i-1][16]


        if duration >= args.window:
            moni_gaps.append((start, end, duration, tiu, daq, t, r, lr, time_elapsed, nevents, evq, ev_unsent, ev_missed, mte_rec, rbe_rec, mte_skipped, time_out, cache, evt_id_cache))


    print('--------------------------------------------------------------------------------------------')
    print('Detected ' + str(len(moni_gaps))+ ' MTB outages with lenth greater than '+ str(args.window) + ' seconds between ' + str(args.start_time) + ' to '+ str(args.end_time) + ' seconds')
    for gap in moni_gaps:
        print(f'from {gap[0]} to {gap[1]} with duration {gap[2]}')
        print(f'---the tiu_busy_count before the crash was {gap[3]}')
        print(f'---the daq queue length before the crash was {gap[4]}')
        print(f'---the temperature before the crash was {gap[5]}')
        print(f'---the rate before the crash was {gap[6]}')
        print(f'---the lost rate before the crash was {gap[7]}')
        print(f'---the elapsed time before the crash was {gap[8]}')
        print(f'---the num. events received before the crash was {gap[9]}')
        print(f'---the event queue size before the crash was {gap[10]}')
        print(f'---the num. unsent events before the crash was {gap[11]}')
        print(f'---the num. missed events before the crash was {gap[12]}')
        print(f'---the MTEvent receiver len. before the crash was {gap[13]}')
        print(f'---the RBEvent receiver len. before the crash was {gap[14]}')
        print(f'---the num. MTEvents skipped before the crash was {gap[15]}')
        print(f'---the num. timed out events before the crash was {gap[16]}')
        print(f'---the cache size before the crash was {gap[17]}')
        print(f'---the event ID cache size before the crash was {gap[18]}')
        #print(f'---the vccint before the crash was {gap[8]}')
        #print(f'---the vccbram before the crash was {gap[9]}')
        #print(f'---the vaux before the crash was {gap[10]}')
        print('------------------------------------------------')

    print('--------------------------------------------------------------------------------------------')


