#include<assert.h>
#include<stdlib.h>
#include "hashtable.h"

static void h_init(HTab *htab, size_t n){
	assert(n>0 && ((n-1) & n));
	htab->tab = (HNode **)calloc(n, sizeof(HNode *));
	htab->mask = n-1;
	htab->size = 0;
}

static void h_insert(HTab *htab, HNode *node){
	size_t pos = node->hcode & htab->mask;
	HNode *next = htab->tab[pos];
	node->next = next;
	htab->tab[pos]=node;
	htab->size++;
}

static HNode **h_lookup(HTab *htab, HNode *key, bool (*eq)(HNode *, HNode *)){
	if(!htab->tab){
		return NULL;
	}
	size_t pos = key->hcode & htab->mask;
	HNode **from = &htab->tab[pos];
	for(HNode *cur; (cur = *from)!=NULL; from = &cur->next){
		if(cur->hcode == key->hcode && eq(cur, key)){
			return from;
		}
	}
	return NULL;
}



static HNode *h_detach(HTab *htab, HNode **from){
	HNode *node = *from;
	*from = node->next;
	htab->size--;
	return  node;
}

static void hm_help_rehashing(HMap *hmap){
	size_t nwork = 0;
	while(nwork <= k_rehashing_work && hmap->older.size > 0){
		HNode **from  = &hmap->older.tab[hmap->migrate_pos];
		if(!*from){
			hmap->migrate_pos++;
			continue;
		}
		HNode *node = h_detach(&hmap->older, from);
		h_insert(&hmap->newer, node);
		nwork++;
	}
	if(hmap->older.size == 0 && hmap->older.tab){
		free(hmap->older.tab);
		hmap->older = HTab{};
	}
}

static void hm_trigger_rehashing(HMap *hmap){
	hmap->older = hmap->newer;
	h_init(&hmap->newer, (hmap->newer.mask+1)*2);
	hmap->migrate_pos = 0;
}

HNode *hm_lookup(HMap *hmap, HNode *key, bool (*eq)(HNode *, HNode *)){
	hm_help_rehashing(hmap);
	HNode **from = h_lookup(&hmap->newer, key, eq);
	if(!from){
		from = h_lookup(&hmap->older, key, eq);
	}
	return from ? *from:NULL;
}

HNode *hm_delete(HMap *hmap, HNode *key, bool (*eq)(HNode *,HNode *)){
	hm_help_rehashing(hmap);
	if(HNode **from = h_lookup(&hmap->newer, key, eq)){
		return h_detach(&hmap->newer, from);
	}
	if(HNode **from = h_lookup(&hmap->older, key, eq)){
		return h_detach(&hmap->older, from);
	}
	return NULL;
}

void hm_insert(HMap *hmap, HNode *node){
	if(!hmap->newer.tab){
		h_init(&hmap->newer, 4);
	}
	h_insert(&hmap->newer, node);
	if(!hmap->older.tab){
		size_t threshold = (hmap->newer.mask + 1) * k_max_load_factor;
		if(hmap->newer.size >= threshold){
			hm_trigger_rehashing(hmap);
		}
	}
	hm_help_rehashing(hmap);
}

void hm_clear(HMap *hmap){
	free(hmap->newer.tab);
	free(hmap->older.tab);
	*hmap = HMap{};
}

size_t hm_size(HMap *hmap){
	return hmap->older.size + hmap->newer.size;
}

static bool h_foreach(HTab *htab, bool (*f)(HNode *,void *), void *arg){
	for(size_t i=0; htab->mask!=0 && i <= htab->mask; i++){
		for(HNode *node = htab->tab[i];node != NULL; node = node->next){
			if(!f(node, arg)){
				return false;
			}
		}
	}
	return true;
}

void hm_foreach(HMap *hmap, bool (*f)(HNode *,void *), void *arg){
	h_foreach(&hmap->newer, f, arg) && h_foreach(&hmap->older, f, arg);
}
