#include<poll.h>
#include<stdint.h>
#include<assert.h>
#include<stdlib.h>
#include<string.h>
#include<stdio.h>
#include<errno.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<sys/socket.h>
#include<netinet/ip.h>
#include<vector>
#include <fcntl.h>
#include<string>  
#include "hashtable.h"
#include "common.h"
#include "zset.h"
#include<math.h>

using namespace std;

const size_t k_max_msg = 1 << 25;

const size_t k_max_args = 200 * 1000;

static void die(const char *msg){
	fprintf(stderr, "[%d] %s]\n", errno, msg);
	abort();

}

static void msg(const char *msg){
	fprintf(stderr, "%s\n", msg);
}

static void msg_errno(const char *msg){
	fprintf(stderr, "[errno:%d] %s\n", errno, msg);
}

typedef vector<uint8_t> Buffer;

struct Conn{
	int fd = -1;
	bool want_read = false;
	bool want_write = false;
	bool want_close = false;

	Buffer incoming;
	Buffer outgoing;
};

static void fd_set_nb(int fd){
	errno = 0;
	int flags = fcntl(fd, F_GETFL, 0);
	if(errno){
		die("fnctl error");
		return ;
	}

	flags |= O_NONBLOCK;
	errno = 0;
	(void)fcntl(fd, F_SETFL, flags);
	if(errno){
		die("fcntl error");
	}
}

//append to back
static void buf_append(Buffer &buf, const uint8_t *data,size_t len){
	buf.insert(buf.end(),data, data + len);
}

//remove from front
static void buf_consume(Buffer &buf, size_t n){
	buf.erase(buf.begin(), buf.begin() + n);
}

static bool read_u32(const uint8_t*&curr, const uint8_t*end, uint32_t &out){
	if(curr+4 > end){
		return false;
	}
	memcpy(&out, curr, 4);
	curr=curr+4;
	return true;
}

static bool read_str(const uint8_t *&curr, const uint8_t *end, size_t len, string &out){
	if(curr + len > end){
		return false;
	}
	out.assign(curr,curr+len);
	curr+=len;
	return true;
}



enum {
	ERR_UNKNOWN = 1,
	ERR_TOO_BIG = 2,
	ERR_BAD_TYP = 3,
	ERR_BAD_ARG = 4,
};

static void buf_append_u8(Buffer &buf, uint8_t data){
	buf.push_back(data);
}

static void buf_append_u32(Buffer &buf, uint32_t data){
	buf_append(buf, (const uint8_t *)&data, 4);
}

static void buf_append_i64(Buffer &buf, uint64_t data){
	buf_append(buf, (const uint8_t *)&data, 8);
}

static void buf_append_dbl(Buffer &buf, double data) {
    buf_append(buf, (const uint8_t *)&data, 8);
}

static int32_t parse_req(const uint8_t *data, size_t size, vector<string> &out){
	const uint8_t *end = data + size;
	uint32_t nstr = 0;
	if(!read_u32(data, end, nstr)){
		return -1;
	}
	if(nstr > k_max_args){
		return -1;
	}
	while(out.size() < nstr){
		uint32_t len = 0;
		if(!read_u32(data, end, len)){
			return -1;
		}
		out.push_back(string());
		if(!read_str(data, end, (size_t)len, out.back())){
			return -1;
		}
	}
	if(data != end){
		return -1;
	}
	return 0;
}

static struct {
	HMap db;
} g_data;

enum{
	T_INIT = 0,
	T_STR = 1,
	T_ZSET = 2,
};

enum {
	TAG_NIL = 0,
	TAG_ERR = 1,
	TAG_STR = 2,
	TAG_INT = 3,
	TAG_DBL = 4,
	TAG_ARR = 5,
};

struct Entry {
	struct HNode node;
	std::string key;
	uint32_t type = 0;
	string str;
	ZSet zset;
};

static Entry *entry_new(uint32_t type){
	Entry *ent = new Entry();
	ent->type = type;
	return ent;
}

static void entry_del(Entry *ent){
	if(ent->type == T_ZSET){
		zset_clear(&ent->zset);
	}
	delete ent;
}

struct LookupKey{
	HNode node;
	string key;
};

static bool entry_eq(HNode *node,HNode *key){
	struct Entry *ent = container_of(node, struct Entry, node);
	struct LookupKey *keydata = container_of(key, struct LookupKey, node);
	return ent->key == keydata->key;
}

static void out_nil(Buffer &out){
	buf_append_u8(out, TAG_NIL);
}

static void out_str(Buffer &out, const char *s, size_t size){
	buf_append_u8(out, TAG_STR);
	buf_append_u32(out, (uint32_t)size);
	buf_append(out, (const uint8_t *)s, size);
}

static void out_int(Buffer &out, uint64_t val){
	buf_append_u8(out, TAG_INT);
	buf_append_i64(out, val);
}

static void out_arr(Buffer &out, size_t n){
	buf_append_u8(out, TAG_ARR);
	buf_append_u32(out, (uint32_t)n);
}

static size_t out_begin_arr(Buffer &out){
	buf_append_u8(out, TAG_ARR);
	buf_append_u32(out, 0);
	return out.size() - 4;
}

static void out_end_arr(Buffer &out, size_t cntx, uint32_t n){
	assert(out[cntx-1] == TAG_ARR);
	memcpy(&out[cntx], &n, 4);
}

static void out_dbl(Buffer &out, double val){
	buf_append_u8(out, TAG_DBL);
	buf_append_dbl(out, val);
}

static void out_err(Buffer &out, uint32_t code, const string &msg){
	buf_append_u8(out, TAG_ERR);
	buf_append_u32(out, code);
	buf_append_u32(out, (uint32_t)msg.length());
	buf_append(out, (const uint8_t *)msg.data(), size_t(msg.length()));
}

static void do_get(vector<string> &cmd, Buffer &out){
	LookupKey key;
	key.key.swap(cmd[1]);
	key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
	HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
	if(!node){
		return out_nil(out);
	}

	Entry *ent = container_of(node, struct Entry, node);
	if(ent->type != T_STR){
		return out_err(out, ERR_BAD_TYP, "not a string");
	}
	return out_str(out, ent->str.data(), ent->str.size());
}

static void do_set(vector<string> &cmd, Buffer &out){
	LookupKey key;
	key.key.swap(cmd[1]);
	key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
	HNode *node = hm_lookup(&g_data.db, &key.node, &entry_eq);
	if(!node){
		Entry *ent = entry_new(T_STR);
		ent->key.swap(key.key);
		ent->node.hcode = key.node.hcode;
		ent->str.swap(cmd[2]);
		hm_insert(&g_data.db, &ent->node);
	}
	else{
		Entry *ent = container_of(node, struct Entry, node);
		if(ent->type != T_STR){
			return out_err(out, ERR_BAD_TYP, "not a string");
		}
		ent->str.swap(cmd[2]);
	}
	return out_nil(out);

}

static void do_del(vector<string> &cmd, Buffer &out){
	LookupKey key;
	key.key.swap(cmd[1]);
	key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
	HNode *node = hm_delete(&g_data.db, &key.node, &entry_eq);
	if(node){
		Entry *ent = container_of(node, struct Entry, node);
		entry_del(ent);
	}
	return out_int(out, node ? 1 : 0);
}

static bool cb_keys(HNode *node, void *arg){
	Buffer &out = *(Buffer *)arg;
	string &key = container_of(node, Entry, node)->key;
	out_str(out, key.data(), key.size());
	return true;
}

static void do_keys(vector<string> &cmd, Buffer &out){
	out_arr(out, (uint32_t)hm_size(&g_data.db));
	hm_foreach(&g_data.db, &cb_keys, (void *)&out);
}

static bool str2dbl(const std::string &s, double &out) {
    char *endp = NULL;
    out = strtod(s.c_str(), &endp);
    return endp == s.c_str() + s.size() && !isnan(out);
}

static bool str2int(const std::string &s, int64_t &out) {
    char *endp = NULL;
    out = strtoll(s.c_str(), &endp, 10);
    return endp == s.c_str() + s.size();
}

// zadd zset score name
static void do_zadd(vector<string> &cmd, Buffer &out){
	double score = 0;
	if(!str2dbl(cmd[2], score)){
		out_err(out, ERR_BAD_ARG, "expect float");
	}
	LookupKey key;
	key.key.swap(cmd[1]);
	key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
	HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
	Entry *ent = NULL;
	if(!hnode){
		ent = entry_new(T_ZSET);
		ent->node.hcode = key.node.hcode;
		ent->key.swap(key.key);
		hm_insert(&g_data.db, &ent->node);
	}
	else{
		ent = container_of(hnode, struct Entry, node);
		if(ent->type != T_ZSET){
			return out_err(out, ERR_BAD_TYP, "not a zset");
		}
	}
	const string &name = cmd[3];
	bool added = zset_insert(&ent->zset, name.data(), name.size(), score);
	return out_int(out, (uint64_t)added);

}

static const ZSet k_empty_set;

static ZSet *expect_zset(string &s){
	LookupKey key;
	key.key.swap(s);
	key.node.hcode = str_hash((uint8_t *)key.key.data(), key.key.size());
	HNode *hnode = hm_lookup(&g_data.db, &key.node, &entry_eq);
	if(!hnode){
		return (ZSet *)&k_empty_set; 
	}
	Entry *ent = container_of(hnode, Entry, node);
	return ent->type == T_ZSET ? &ent->zset : NULL;
}

static void do_zrem(vector<string> &cmd, Buffer &out){
	ZSet *zset = expect_zset(cmd[1]);
	if(!zset){
		return out_err(out, ERR_BAD_TYP, "expect zset");
	}
	const string &name = cmd[2];
	ZNode *znode = zset_lookup(zset, name.data(), name.size());
	if(znode){
		zset_delete(zset, znode);
	}
	return out_int(out, znode ? 1 : 0);
}

static void do_zscore(vector<string> &cmd, Buffer &out){
	ZSet *zset = expect_zset(cmd[1]);
	if(!zset){
		return out_err(out, ERR_BAD_TYP, "expect zset");
	}
	const string &name = cmd[2];
	ZNode *znode = zset_lookup(zset, name.data(), name.size());
	return znode ? out_dbl(out, znode->score) : out_nil(out);
}
// zquery zset score name offset limit
static void do_zquery(vector<string> &cmd, Buffer &out){
	double score = 0;
	if(!str2dbl(cmd[2], score)){
		return out_err(out, ERR_BAD_ARG, "expect float");
	}
	const string &name = cmd[3];
	int64_t offset = 0, limit = 0;
	if(!str2int(cmd[4], offset) || !str2int(cmd[5], limit)){
		return out_err(out,ERR_BAD_ARG, "expect integer");
	}
	if(limit <= 0){
		return out_arr(out, 0);
	}
	ZSet *zset = expect_zset(cmd[1]);
	if(!zset){
		return out_err(out, ERR_BAD_TYP, "expect zset");
	}
	ZNode *znode = zset_seekge(zset, score, name.data(), name.size());
	znode = znode_offset(znode, offset);
	size_t ctx = out_begin_arr(out);
	int64_t n = 0;
	while(znode && n < limit){
		out_str(out, znode->name, znode->len);
		out_dbl(out, znode->score);
		znode = znode_offset(znode, +1);
		n += 2;
	}
	return out_end_arr(out, ctx, (uint32_t)n);
}

static void do_request(vector<string> &cmd, Buffer &out){
	if(cmd.size() == 2 && cmd[0] == "get"){
		return do_get(cmd,out);		
	}
	else if(cmd.size() == 3 && cmd[0] == "set"){
		return do_set(cmd,out);
	}
	else if(cmd.size() == 2 && cmd[0] == "del"){
		return do_del(cmd,out);
	}
	else if(cmd.size() == 1 && cmd[0] == "keys"){
		return do_keys(cmd, out);
	}
	else if(cmd.size() == 4 && cmd[0] == "zadd"){
		return do_zadd(cmd, out);
	}
	else if(cmd.size() == 3 && cmd[0] == "zrem"){
		return do_zrem(cmd, out);
	}
	else if(cmd.size() == 3 && cmd[0] == "zscore"){
		return do_zscore(cmd, out);
	}
	else if(cmd.size() == 6 && cmd[0] == "zquery"){
		return do_zquery(cmd, out);
	}
	else{
		return out_err(out, ERR_UNKNOWN, "unknown command.");
	}
}

static void response_begin(Buffer &out, size_t *header){
	*header = out.size();
	buf_append_u32(out, 0);
}

static size_t response_size(Buffer &out, size_t header){
	return out.size() - header - 4;
}

static void response_end(Buffer &out, size_t header){
	size_t msg_size = response_size(out, header);
	if(msg_size > k_max_msg){
		out.resize(header+4);
		out_err(out, ERR_TOO_BIG, "response is too big.");
		msg_size = response_size(out, header);
	}
	uint32_t len = (uint32_t)msg_size;
	memcpy(&out[header], &len, 4);
}

static Conn *handle_accept(int fd){
	//accept
	struct sockaddr_in client_addr = {};
	socklen_t addrlen = sizeof(client_addr);
	int connfd = accept(fd, (struct sockaddr *)&client_addr, &addrlen);
	if(connfd < 0){
		msg_errno("accept() error");
		return NULL;
	}
	uint32_t ip = client_addr.sin_addr.s_addr;
	fprintf(stderr, "new client from %u.%u.%u.%u:%u\n",
        ip & 255, (ip >> 8) & 255, (ip >> 16) & 255, ip >> 24,
        ntohs(client_addr.sin_port)
 	   );
	//set the connection fd to nonblocking mode
	fd_set_nb(connfd);
	//create Conn struct
	Conn *conn = new Conn();
	conn->fd = connfd;
	conn->want_read = true;
	return conn;
}

static bool try_one_request(Conn *conn){
	//try parsing the incoming buffer
	if(conn->incoming.size() < 4){
		return false;
	}
	uint32_t len = 0;
	memcpy(&len, conn->incoming.data(), 4);
	if(len > k_max_msg){
		msg("too long");
		conn->want_close = true;
		return false;
	}
	if(4 + len > conn->incoming.size()){
		return false;
	}
	uint8_t *request = &conn->incoming[4];
	vector<string> cmd;
	if(parse_req(request, len, cmd) < 0){
		msg("bad request");
		conn->want_close = true;
		return false;
	}
	size_t header_pos = 0;
	response_begin(conn->outgoing, &header_pos);
	do_request(cmd, conn->outgoing);
	response_end(conn->outgoing, header_pos);

	buf_consume(conn->incoming, 4+len);

	return true;
	
}

static void handle_write(Conn *conn){
	assert(conn->outgoing.size() > 0);
	ssize_t rv = write(conn->fd, conn->outgoing.data(), conn->outgoing.size());
	if(rv < 0 && errno == EAGAIN){
		return ;
	}
	if(rv < 0){
		msg_errno("write() error");
		conn->want_close = true;
		return ;
	}
	buf_consume(conn->outgoing, (size_t)rv);
	if (conn->outgoing.size() ==0){
		conn->want_read = true;
		conn->want_write = false;
	}
}

static void handle_read(Conn *conn){
	uint8_t buf[64 * 1024];
	ssize_t rv = read(conn->fd, buf, sizeof(buf));
	if(rv < 0 && errno == EAGAIN){
		//  handle execption here
		return ;
	}
	if(rv < 0){
		msg_errno("read() error");
		conn->want_close = true;
		return ;
	}
	if(rv == 0){
		if(conn->incoming.size() == 0){
			msg("client closed");
		}
		else{
			msg("unexpected EOF");
		}
		conn->want_close = true;
		return ;
	}

	buf_append(conn->incoming, buf, (size_t)rv);

	//Parse the buffer and remove the message if any
	while(try_one_request(conn)){}

	if(conn->outgoing.size() > 0){
		conn->want_read = false;
		conn->want_write = true;
		return handle_write(conn);
	}

}

int main(){
	// create socket for listening
	int fd = socket(AF_INET,SOCK_STREAM,0);
	if(fd < 0){
		die("socket()");
	}
	int val = 1;
	setsockopt(fd,SOL_SOCKET,SO_REUSEADDR,&val,sizeof(val));
	struct sockaddr_in addr = {};
	addr.sin_family = AF_INET;
	addr.sin_port = htons(1234);
	addr.sin_addr.s_addr=htonl(0);
	//bind
	int rv = bind(fd, (const sockaddr *) &addr, sizeof(addr));
	if(rv){
		die("bind()");
	}
	//listen
	rv = listen(fd, SOMAXCONN);
	if(rv){
		die("listen()");
	}
	//map of fds and connection state objects
	vector<Conn *> fd2conn;
	//event loop
	vector<struct pollfd> poll_args;
	while(true){
		// prepare arguement for poll
		poll_args.clear();
		//put the listening socket in the first position
		struct pollfd pfd = {fd, POLLIN, 0};
		poll_args.push_back(pfd);
		//add connection sockets
		for(Conn *conn : fd2conn){
			if(!conn){
				continue;
			}
			struct pollfd pfd = {conn->fd, POLLERR, 0};

			if(conn->want_write){
				pfd.events |=POLLOUT;
			}
			if(conn->want_read){
				pfd.events |=POLLIN;
			}
			poll_args.push_back(pfd);
		}
		//wait for readiness
		ssize_t rv = poll(poll_args.data(), (nfds_t)poll_args.size(), -1);
		if(rv<0 && errno == EINTR){
			continue;
		}
		if(rv < 0){
			die("poll()");
		}
		//handle the listening socket
		if(poll_args[0].revents ){
			if(Conn *conn = handle_accept(fd)){
				if(fd2conn.size()<=(size_t)conn->fd){
					fd2conn.resize(conn->fd + 1);
				}
				assert(!fd2conn[conn->fd]);
				fd2conn[conn->fd]=conn;
			}
		}
		//handle connection sockets
		for(size_t i = 1;i < poll_args.size(); i++){
			uint32_t ready = poll_args[i].revents;
			if(ready == 0){
				continue;
			}
			Conn *conn = fd2conn[poll_args[i].fd];
			if(ready & POLLIN){
				assert(conn->want_read);
				handle_read(conn);
			}
			if(ready & POLLOUT){
				assert(conn->want_write);
				handle_write(conn);
			}
			if((ready & POLLERR) || conn->want_close){
				(void)close(conn->fd);
				fd2conn[conn->fd] = NULL;
				delete conn;
			}
		}
		
	}
	return 0;
}
