#include <asm-generic/int-ll64.h>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <liburing.h>
#include <sys/socket.h>
#include <thread>
#include "include/comm.h"
#include "include/config.hpp"
#include "include/engine.hpp"
#include "include/time.hpp"
#include "include/data.hpp"
#include "include/util.hpp"

enum CqeType { 
  SEND_DATA = 0,
  RECV_DATA,
  SEND_RESP,
  RECV_RESP,
  CQE_TYPE_CNT  
};

void print_elapse(const char *desc, const duration_t &dur) {
  DEBUG_PRINTF(0, "%s: %s elapse time: %lf s\n",
    this_host_info, desc, dur.count());
}

void RemoteState::open(std::string fname, bool *is_new_create) {
  next_user_index = static_cast<volatile uint32_t *>(map_file(fname.c_str(), MAX_NR_CONSUMER * sizeof(uint32_t), is_new_create));
}

void Engine::do_peer_data_sync() {
  duration_t dsync_time(0);
  start_time_record();

  DataTransMeta send_meta[MAX_NR_CONSUMER];
  DataTransMeta recv_meta[MAX_NR_CONSUMER];
  uint32_t send_resp[MAX_NR_CONSUMER];
  uint32_t recv_resp[MAX_NR_CONSUMER];
  UserArray *send_src[MAX_NR_CONSUMER];
  ArrayTransControl send_data_ctrl, recv_data_ctrl;
  TransControl send_meta_ctrl, recv_meta_ctrl, 
    send_resp_ctrl, recv_resp_ctrl;

  bool send_nothing = true,
    recv_nothing = true;

  DEBUG_PRINTF(0, "%s start peer data sync\n", this_host_info);

  memset(send_meta, 0, sizeof(send_meta));
  memset(recv_meta, 0, sizeof(recv_meta));
  memset(send_resp, 0, sizeof(send_resp));
  memset(recv_resp, 0, sizeof(recv_resp));
  memset(&send_data_ctrl, 0, sizeof(send_data_ctrl));
  memset(&recv_data_ctrl, 0, sizeof(recv_data_ctrl));
  memset(&send_meta_ctrl, 0, sizeof(send_meta_ctrl));
  memset(&recv_meta_ctrl, 0, sizeof(recv_meta_ctrl));
  memset(&send_resp_ctrl, 0, sizeof(send_resp_ctrl));
  memset(&recv_resp_ctrl, 0, sizeof(recv_resp_ctrl));

  send_meta_ctrl.name = "send_meta";
  recv_meta_ctrl.name = "recv_meta";
  send_resp_ctrl.name = "send_resp";
  recv_resp_ctrl.name = "recv_resp";
  send_data_ctrl.name = "send_data";
  recv_data_ctrl.name = "recv_data";

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    auto remote_tail_value = remote_state.get_next_user_index()[i];
    auto head_value = qs[i].head->load();

    if (head_value == remote_tail_value) {
      continue;
    }

    send_nothing = false;

    auto start_ca_pos = remote_tail_value / UserArray::N_DATA;
    auto end_ca_pos = ROUND_DIV(head_value, UserArray::N_DATA);
    auto ca_cnt = end_ca_pos - start_ca_pos;

    send_meta[i].ca_start = start_ca_pos;
    send_meta[i].ca_cnt = ca_cnt;
    send_meta[i].user_start = remote_tail_value;
    send_meta[i].user_cnt = head_value - remote_tail_value;
    send_data_ctrl.ctrls[i].src = (char *)&datas[i].get_pmem_users()[start_ca_pos];
    send_data_ctrl.ctrls[i].rest = ca_cnt * UserArray::DALIGN;
  }

  send_meta_ctrl.src = (char *)&send_meta;
  send_meta_ctrl.rest = sizeof(send_meta);
  recv_meta_ctrl.src = (char *)&recv_meta;
  recv_meta_ctrl.rest = sizeof(recv_meta);
  
  int ret = send_all(data_fd, send_meta_ctrl.src, send_meta_ctrl.rest, MSG_NOSIGNAL);
  assert(ret == send_meta_ctrl.rest);
  DEBUG_PRINTF(0, "%s send_meta done\n", this_host_info);
  /* do_send_meta(send_meta_ctrl); */
  ret = recv_all(data_recv_fd, recv_meta_ctrl.src, recv_meta_ctrl.rest, MSG_WAITALL);
  assert(ret == recv_meta_ctrl.rest);
  DEBUG_PRINTF(0, "%s recv_meta done\n", this_host_info);
  finish_recv_meta(recv_meta, recv_data_ctrl);
  /* do_recv_meta(recv_meta_ctrl); */

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    DEBUG_PRINTF(0, "\t[%d] ca: [%d, %d), user: [%d, %d)\n", i, 
      send_meta[i].ca_start, send_meta[i].ca_start + send_meta[i].ca_cnt,
      send_meta[i].user_start, send_meta[i].user_start + send_meta[i].user_cnt);
  }

  bool need_to_send = false,
       need_to_recv = false;

  for (int i = 0; i < MAX_NR_CONSUMER; ++i) {
    if (send_meta[i].user_cnt > 0) {
      need_to_send = true;
      break;
    }
  }

  for (int i = 0; i < MAX_NR_CONSUMER; ++i) {
    if (recv_meta[i].user_cnt > 0) {
      need_to_recv = true;
      break;
    }
  }
  
  auto recv_fn = [&]() {
    while (true) {
      auto cc = recv_data_ctrl.ctrls[recv_data_ctrl.cur];
      int ret = recv_all(data_recv_fd, cc.src, cc.rest, MSG_WAITALL);
      assert(ret == cc.rest);
      if (recv_data_ctrl.update_check_finished(ret))
        break;
    }
    finish_recv_data(recv_meta);
  };
  std::thread *reciver = nullptr;
  if (need_to_recv) {
    reciver = new std::thread(recv_fn);
  }

  if (need_to_send) {
    while (true) {
      auto cc = send_data_ctrl.ctrls[send_data_ctrl.cur];
      ret = send_all(data_fd, cc.src, cc.rest, MSG_NOSIGNAL);
      assert(ret == cc.rest);
      if (send_data_ctrl.update_check_finished(ret))
        break;
    }
  }
  if (reciver) {
    reciver->join();
    delete reciver;
    reciver = nullptr;
  }

  // response
  uint32_t remote_lasts[MAX_NR_CONSUMER];
  memset(remote_lasts, 0, sizeof(remote_lasts));
  send_resp_ctrl.src = (char *)remote_state.get_next_user_index();
  send_resp_ctrl.rest = MAX_NR_CONSUMER * sizeof(uint32_t);
  recv_resp_ctrl.src = (char *)remote_lasts;
  recv_resp_ctrl.rest = MAX_NR_CONSUMER * sizeof(uint32_t);

  auto recv_resp_fn = [&]() {
    while (true) {
      int ret = recv_all(data_recv_fd, recv_resp_ctrl.src, recv_resp_ctrl.rest, MSG_WAITALL);
      assert(ret == recv_resp_ctrl.rest);
      if (recv_resp_ctrl.update_check_finished(ret))
        break;
    }
    finish_recv_resp(remote_lasts);
  };
  std::thread *resp_reciver = nullptr;
  if (need_to_send) {
    DEBUG_PRINTF(0, "%s start recv_resp\n", this_host_info);
    resp_reciver = new std::thread(recv_resp_fn);
  }

  if (need_to_recv) {
    DEBUG_PRINTF(0, "%s start send_resp:\n", this_host_info);
    while (true) {
      ret = send_all(data_fd, send_resp_ctrl.src, send_resp_ctrl.rest, MSG_NOSIGNAL);
      assert(ret == recv_resp_ctrl.rest);
      if (send_resp_ctrl.update_check_finished(ret))
        break;
    }
    /* do_send_resp(send_resp_ctrl); */
  }

  if (resp_reciver) {
    resp_reciver->join();
    delete resp_reciver;
    resp_reciver = nullptr;
  }

  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    DEBUG_PRINTF(0, "\t[%d] next_user_index = %d\n", i, remote_state.get_next_user_index()[i]);
  }

  end_time_record(&dsync_time);
  print_elapse("peer data sync", dsync_time);
  duration_t sync_flag_time(0);
  start_time_record();

  end_time_record(&sync_flag_time);
  print_elapse("remote mmap sync", sync_flag_time);
}

/* void Engine::do_send_data(const ArrayTransControl &ctrl) { */
/*   auto cc = ctrl.ctrls[ctrl.cur]; */
/*   add_write_request(data_ring, data_fd, cc.src, cc.rest, (__u64)SEND_DATA); */
/* } */

/* void Engine::do_recv_data(ArrayTransControl &ctrl) { */
/*   auto cc = ctrl.ctrls[ctrl.cur]; */
/*   add_read_request(data_ring, data_recv_fd, cc.src, cc.rest, (__u64)RECV_DATA); */
/* } */

/* void Engine::do_send_resp(const TransControl &ctrl) { */
/*   add_write_request(data_ring, data_fd, ctrl.src, ctrl.rest, (__u64)SEND_RESP); */
/* } */

/* void Engine::do_recv_resp(TransControl &ctrl) { */
/*   add_read_request(data_ring, data_recv_fd, ctrl.src, ctrl.rest, (__u64)RECV_RESP); */
/* } */

bool Engine::finish_recv_meta(const DataTransMeta *recv_meta, ArrayTransControl &recv_data_ctrl) {
  bool recv_nothing = true;
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    recv_data_ctrl.ctrls[i].src = (char *)&remote_datas[i].get_pmem_users()[recv_meta[i].ca_start];
    recv_data_ctrl.ctrls[i].rest = recv_meta[i].ca_cnt * UserArray::DALIGN;
    if (recv_meta[i].ca_cnt != 0 && recv_nothing) {
      recv_nothing = false;
      recv_data_ctrl.cur = i;
    }
  }
  return recv_nothing;
}

void Engine::finish_recv_data(const DataTransMeta *recv_metas) {
  for (int qid = 0; qid < MAX_NR_CONSUMER; qid++) {
    auto &remote_next = remote_state.get_next_user_index()[qid];
    auto &remote_data = remote_datas[qid];
    const auto &recv_meta = recv_metas[qid];
    auto end = recv_meta.user_start + recv_meta.user_cnt;

    assert(recv_meta.user_start <= remote_next && remote_next <= end);

    for (auto i = remote_next; i < end; i++) {
      const User *user = remote_data.data_read(i);
      uint32_t encoded_index = (qid << 28) | i;
      
      remote_id_r->put(user->id, encoded_index);
      remote_uid_r->put(std::hash<UserString>()(*(UserString *)(user->user_id)), encoded_index);
      remote_sala_r->put(user->salary, encoded_index);
      remote_data.put_flag(i);
      /* char uid[128]; */
      /* assert(remote_id_r->get(&user->id, Id, Userid, uid, false) == 1); */
      /* assert(std::string(uid) == std::string(user->user_id)); */
      /* int64_t key; */
      /* assert(remote_id_r->get(user->user_id, Userid, Id, &key, false) == 1); */
      /* assert(key == user->id); */
    }
    remote_next = end;
  }
}

void Engine::finish_recv_resp(uint32_t *newest_remote_next) {
  auto remote_next_user_index = remote_state.get_next_user_index();
  for (int i = 0; i < MAX_NR_CONSUMER; i++) {
    auto head_value = qs[i].head->load();
    if (newest_remote_next[i] != head_value) {
      DEBUG_PRINTF(0, "%s: newest_remote_next[%d](= %d) != head_value(= %ld)\n",
                   this_host_info, i, newest_remote_next[i], head_value);
      assert(newest_remote_next[i] == head_value);
    }
    remote_next_user_index[i] = head_value;
  }
}
