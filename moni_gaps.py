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

                    packet_ts.append((gcu, tiu_busy, daq_queue, temp, rate, lost_rate)) #total_elapsed, n_events, evq_n_last, n_ev_unsent, n_ev_missed, n_mte_received, n_rbe_received, n_mte_skipped, n_timed_out, cache_size, evt_id_cache_size))


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


        if duration >= args.window:
            moni_gaps.append((start, end, duration, tiu, daq, t, r, lr)) #time_elapsed, nevents, evq, ev_unsent, ev_missed, mte_rec, rbe_rec, mte_skipped, time_out, cache, evt_id_cache))

    
    for i in range(len(moni_gaps)):
        t_start = moni_gaps[i][0]
        new_bins = go.io.get_telemetry_binaries((t_start - 300), t_start, data_dir=args.telemetry_dir)

        evt_hb_list = []
        mtb_hb_list = []

        for binary in new_bins:
            treader = go.io.TelemetryPacketReader(str(binary))
            for p in treader:
                if int(p.header.packet_type) == 92: #AnyTofHKP
                    tp_hb = go.io.TofPacket()
                    tp_hb.from_bytestream(p.payload, 0)
                  
                    if int(tp_hb.packet_type) == 62: #MTBHeartbeat
                        mtb_hb = go.tof.monitoring.MTBHeartbeat()
                        mtb_hb.from_tofpacket(tp_hb)
                        mtb_hb_list.append(mtb_hb)
                    
                    if int(tp_hb.packet_type) == 63: #EVTBLDRHeartbeat
                        evt_hb = go.tof.monitoring.EVTBLDRHeartbeat()
                        evt_hb.from_tofpacket(tp_hb)
                        evt_hb_list.append(evt_hb)
        if mtb_hb_list:
            mtb_hb = mtb_hb_list[-1]
            mtb_data = (
                mtb_hb.total_elapsed,
                mtb_hb.n_events,
                mtb_hb.evq_num_events_last,
                mtb_hb.n_ev_unsent,
                mtb_hb.n_ev_missed
                )
        else:
            mtb_data = (None, None, None, None, None)

 
        if evt_hb_list:
            evt_hb = evt_hb_list[-1]
            evt_data = (
                evt_hb.n_mte_received_tot,
                evt_hb.n_rbe_received_tot,
                evt_hb.n_mte_skipped,
                evt_hb.n_timed_out,
                evt_hb.event_cache_size,
                evt_hb.event_cache_size
            )
        else:
            evt_data = (None, None, None, None, None, None)

        moni_gaps[i] = (*moni_gaps[i], mtb_data, evt_data)
                                          

    print('--------------------------------------------------------------------------------------------')
    print('Detected ' + str(len(moni_gaps))+ ' MTB outages with lenth greater than '+ str(args.window) + ' seconds between ' + str(args.start_time) + ' to '+ str(args.end_time) + ' seconds')
    for gap in moni_gaps:
        print(f'from {gap[0]} to {gap[1]} with duration {gap[2]}')
        print(f'---the tiu_busy_count before the crash was {gap[3]}')
        print(f'---the daq queue length before the crash was {gap[4]}')
        print(f'---the temperature before the crash was {gap[5]}')
        print(f'---the rate before the crash was {gap[6]}')
        print(f'---the lost rate before the crash was {gap[7]}')

        print(f'---the elapsed time before the crash was {gap[8][0]}')
        print(f'---the num. events received before the crash was {gap[8][1]}')
        print(f'---the event queue size before the crash was {gap[8][2]}')
        print(f'---the num. unsent events before the crash was {gap[8][3]}')
        print(f'---the num. missed events before the crash was {gap[8][4]}')

        print(f'---the MTEvent receiver len. before the crash was {gap[9][0]}')
        print(f'---the RBEvent receiver len. before the crash was {gap[9][1]}')
        print(f'---the num. MTEvents skipped before the crash was {gap[9][2]}')
        print(f'---the num. timed out events before the crash was {gap[9][3]}')
        print(f'---the cache size before the crash was {gap[9][4]}')
        print(f'---the event ID cache size before the crash was {gap[9][5]}')
        #print(f'---the vccint before the crash was {gap[8]}')
        #print(f'---the vccbram before the crash was {gap[9]}')
        #print(f'---the vaux before the crash was {gap[10]}')
        print('------------------------------------------------')

    print('--------------------------------------------------------------------------------------------')


