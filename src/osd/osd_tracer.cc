// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_tracer.h"

namespace tracing {
namespace osd {

#ifdef HAVE_JAEGER
thread_local tracing::Tracer tracer("osd");
#else // !HAVE_JAEGER
tracing::Tracer tracer;
#endif

} // namespace osd
} // namespace tracing
