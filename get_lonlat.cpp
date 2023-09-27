/**
 * SPDX-License-Identifier: GPL-2.0-or-later
 */

#include <iostream>
#include <iomanip>
#include "node-persistent-cache.hpp"

int main(int argc, char *argv[]) {
    if (argc<2) {
        std::cerr << "Usage: "<<argv[0]<<" [flatondes_file]" << std::endl;
        std::cerr << "\tThen write one {osm_id} per input line," << std::endl;
        std::cerr << "\tWill print one-line results: {lon};{lat};{osm_id}" << std::endl;
        return 0;
    }
    // initialize, for now needs argv[1] the filename to be readwrite access, O_RDWR.
    // changing to O_RDONLY does not compile and is a deeper issue than a oneline change
    node_persistent_cache n(argv[1], false);
    osmid_t osm_id;
    std::cin>>osm_id;
    while (!std::cin.eof()) {
        try {
            osmium::Location result=n.get(osm_id);
            // write Location.lat and Location.lon to stdout, ';'-separated
            result.as_string(std::ostream_iterator<char>(std::cout), ';');
            // add osm_id
            std::cout << ';' << osm_id << std::endl;
        } catch (osmium::invalid_location &err) {
          // don't clutter stderr
          //std::cerr<<"skipping id "<<osm_id<<'\n';
        }
        // continually read new ids until EOF: don't wait until stdin is
        // closed before processing all the ids
        std::cin>>osm_id;
    }
    return 0;
}
