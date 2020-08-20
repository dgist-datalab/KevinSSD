/* 
 * File:   lrucache.hpp
 * Author: Alexander Ponomarev
 *
 * Created on June 20, 2013, 5:09 PM
 */

#ifndef _LRUCACHE_HPP_INCLUDED_
#define	_LRUCACHE_HPP_INCLUDED_

#include <map>
#include <list>
#include <cstddef>
#include <stdexcept>
#include <stdint.h>
#include "../utils.h"

namespace cache {

template<typename key_t, typename value_t>
class lru_cache {
public:
	typedef typename std::pair<key_t, value_t> key_value_pair_t;
	typedef typename std::list<key_value_pair_t>::iterator list_iterator_t;

	lru_cache(size_t max_size) :
		_max_size(max_size) {
			free_kv_pair=NULL;
			measure_init(&my_time);
	}

	lru_cache(size_t max_size, void (*free_kv)(key_t *, value_t *)) :
		_max_size(max_size), free_kv_pair(free_kv){}
	
	const std::pair<key_t, value_t> put(const key_t& key, const value_t& value) {
		auto it = _cache_items_map.find(key);
		_cache_items_list.push_front(key_value_pair_t(key, value));
		if (it != _cache_items_map.end()) {
			_cache_items_list.erase(it->second);
			_cache_items_map.erase(it);
		}
		_cache_items_map[key] = _cache_items_list.begin();
		
		if (_cache_items_map.size() > _max_size) {
			return remove_last_item();
		}
		return std::pair<key_t, value_t> (-1,NULL);
	}

	const value_t get(const key_t& key) {
		auto it = _cache_items_map.find(key);
		if (it == _cache_items_map.end()) {
			//throw std::range_error("There is no such key in cache");
			return nullptr;
		} else {
			_cache_items_list.splice(_cache_items_list.begin(), _cache_items_list, it->second);
			return it->second->second;
		}
	}
	
	bool exists(const key_t& key) const {
		return _cache_items_map.find(key) != _cache_items_map.end();
	}


	void resize(size_t size){
		while(_cache_items_map.size()>size){
			remove_last_item();
		}
		_max_size=size;
	}

	size_t size() const {
		return _cache_items_map.size();
	}
	~lru_cache(){
		printf("my_time:");
		measure_adding_print(&my_time);
	}
private:
	std::list<key_value_pair_t> _cache_items_list;
	std::map<key_t, list_iterator_t> _cache_items_map;
	size_t _max_size;
	void (*free_kv_pair)(key_t* , value_t *);

	const std::pair<key_t, value_t> remove_last_item(){
		MS(&my_time);
		auto last = _cache_items_list.end();
		last--;
		std::pair<key_t, value_t> res=*last;
		if(free_kv_pair){
			free_kv_pair(&last->first, &last->second);
		}
		_cache_items_map.erase(last->first);
		_cache_items_list.pop_back();
		MA(&my_time);
		return res;
	}
	MeasureTime my_time;
};

} // namespace cache

#endif	/* _LRUCACHE_HPP_INCLUDED_ */
