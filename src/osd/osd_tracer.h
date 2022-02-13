// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "common/tracer.h"

namespace tracing {
namespace osd {

#ifdef HAVE_JAEGER
extern thread_local tracing::Tracer tracer;
#else
extern tracing::Tracer tracer;
#endif

} // namespace osd
} // namespace tracing
