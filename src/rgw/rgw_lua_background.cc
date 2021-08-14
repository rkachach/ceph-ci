#include "rgw_lua_background.h"
#include "rgw_lua.h"
#include "rgw_lua_utils.h"
#include "include/ceph_assert.h"
#include <lua.hpp>

namespace rgw::lua {

void Background::shutdown(){
  this->stop();
  runner.join();
}
void Background::stop(){
  stopped = true;
}

//(1) Loads the script from the object
//(2) Executes the script
//(3) Sleep (configurable)
void Background::run() {
  this->create_background_metatable(L);

  while(!stopped) {

    std::string tenant;
    auto rc = rgw::lua::read_script(dpp, store, tenant, null_yield, rgw::lua::context::background, rgw_script);
    if (rc == -ENOENT) {
      //no script, nothing to do
    } else if (rc < 0) {
      ldpp_dout(dpp, 1) << "WARNING: failed to read background script. error " << rc << dendl;
    } else {
      try {
        //execute the background lua script
        if (luaL_dostring(L, rgw_script.c_str()) != LUA_OK) {
          const std::string err(lua_tostring(L, -1));
          ldpp_dout(dpp, 1) << "Lua ERROR: " << err << dendl;
        }
      } catch (const std::runtime_error& e) {
         ldpp_dout(dpp, 1) << "Lua ERROR: " << e.what() << dendl;
      }
    }
    try {
      if(const auto x = std::stoi(rgw_map["setoff"]); x > 0) {
        set_off = x;
      }
    } catch (const std::invalid_argument& e) {
    } catch (const std::out_of_range& e) {
    }
    std::this_thread::sleep_for(std::chrono::seconds(set_off));
  }
}

void Background::create_background_metatable(lua_State* L) {
  create_metatable<rgw::lua::RGWTable>(L, true, &rgw_map, &m_mutex);
}

} //namespace lua

