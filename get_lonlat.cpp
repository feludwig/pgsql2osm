/**
 * SPDX-License-Identifier: GPL-2.0-or-later
 */

#include <iostream>
#include <iomanip>
#include "node-persistent-cache.hpp"

int main(int argc, char *argv[])
{

    if (argc<2) {
	std::cerr << "Usage: "<<argv[0]<<" [flatondes_file]" << std::endl;
	std::cerr << "\tThen write one {osm_id} per input line," << std::endl;
	std::cerr << "\tWill print one-line results: {lon};{lat};{osm_id}" << std::endl;
        return 0;
    }
    node_persistent_cache n(argv[1], false);
    osmid_t osm_id;
    //char* osm_id_str_prev=argv[2];
    //char* osm_id_str=argv[2];
    std::cin>>osm_id;
    while (!std::cin.eof()) {
      //osm_id_str=strchr(osm_id_str_prev,',');
      //if (osm_id_str!=NULL) {
        //*osm_id_str='\0';
      //}
      //sscanf(osm_id_str_prev,"%ld",&osm_id);
      //osm_id_str_prev=osm_id_str+1;
      try {
        osmium::Location result=n.get(osm_id);
        result.as_string(std::ostream_iterator<char>(std::cout), ';');
        std::cout << ';' << osm_id << std::endl;
      } catch (osmium::invalid_location &err) {
        //std::cerr<<"skipping id "<<osm_id<<'\n';
      }
      std::cin>>osm_id;
      //if (osm_id_str==NULL) {
        //break;
      //}
    }
    return 0;
}
