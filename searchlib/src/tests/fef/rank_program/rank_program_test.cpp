// Copyright 2016 Yahoo Inc. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
#include <vespa/vespalib/testkit/test_kit.h>
#include <vespa/vespalib/stllike/string.h>
#include <vespa/vespalib/util/stringfmt.h>
#include <vespa/searchlib/features/valuefeature.h>
#include <vespa/searchlib/fef/blueprintfactory.h>
#include <vespa/searchlib/fef/test/indexenvironment.h>
#include <vespa/searchlib/fef/test/queryenvironment.h>
#include <vespa/searchlib/fef/test/plugin/sum.h>
#include <vespa/searchlib/fef/test/plugin/double.h>
#include <vespa/searchlib/fef/rank_program.h>
#include <vespa/searchlib/fef/test/test_features.h>

using namespace search::fef;
using namespace search::fef::test;
using namespace search::features;

uint32_t default_docid = 1;

void maybe_insert(const LazyValue &value, std::vector<LazyValue> &seen) {
    for (const auto &entry: seen) {
        if (value.is_same(entry)) {
            return;
        }
    }
    seen.push_back(value);
}

std::vector<LazyValue> get_features(const RankProgram &program) {
    std::vector<LazyValue> seen;
    auto unboxed = program.get_all_features(true);
    for (size_t i = 0; i < unboxed.num_features(); ++i) {
        maybe_insert(unboxed.resolve(i), seen);
    }
    auto maybe_boxed = program.get_all_features(false);
    for (size_t i = 0; i < maybe_boxed.num_features(); ++i) {
        maybe_insert(maybe_boxed.resolve(i), seen);
    }
    return seen;
}

template <typename Predicate>
size_t count(const RankProgram &program, Predicate pred) {
    size_t cnt = 0;
    for (const auto &value: get_features(program)) {
        if (pred(value)) {
            ++cnt;
        }
    }
    return cnt;
}

size_t count_features(const RankProgram &program) {
    return count(program, [](const LazyValue &){ return true; });
}

size_t count_const_features(const RankProgram &program) {
    return count(program, [](const LazyValue &value){ return value.is_const(); });
}

struct Fixture {
    BlueprintFactory factory;
    IndexEnvironment indexEnv;
    BlueprintResolver::SP resolver;
    Properties overrides;
    RankProgram program;
    size_t track_cnt;
    Fixture() : factory(), indexEnv(), resolver(new BlueprintResolver(factory, indexEnv)),
                overrides(), program(resolver), track_cnt(0)
    {
        factory.addPrototype(Blueprint::SP(new BoxingBlueprint()));
        factory.addPrototype(Blueprint::SP(new DocidBlueprint()));
        factory.addPrototype(Blueprint::SP(new DoubleBlueprint()));
        factory.addPrototype(Blueprint::SP(new ImpureValueBlueprint()));
        factory.addPrototype(Blueprint::SP(new SumBlueprint()));
        factory.addPrototype(Blueprint::SP(new TrackingBlueprint(track_cnt)));        
        factory.addPrototype(Blueprint::SP(new ValueBlueprint()));
    }
    Fixture &add(const vespalib::string &feature) {
        resolver->addSeed(feature);
        return *this;
    }
    Fixture &override(const vespalib::string &feature, double value) {
        overrides.add(feature, vespalib::make_string("%g", value));
        return *this;
    }
    Fixture &compile() {
        ASSERT_TRUE(resolver->compile());
        MatchDataLayout mdl;
        QueryEnvironment queryEnv(&indexEnv);
        program.setup(mdl, queryEnv, overrides);
        return *this;
    }
    double get(uint32_t docid = default_docid) {
        auto result = program.get_seeds();
        EXPECT_EQUAL(1u, result.num_features());
        return result.resolve(0).as_number(docid);
    }
    double get(const vespalib::string &feature, uint32_t docid = default_docid) {
        auto result = program.get_seeds();
        for (size_t i = 0; i < result.num_features(); ++i) {
            if (result.name_of(i) == feature) {
                return result.resolve(i).as_number(docid);
            }
        }
        return 31212.0;
    }
    std::map<vespalib::string, double> all(uint32_t docid = default_docid) {
        auto result = program.get_seeds();
        std::map<vespalib::string, double> result_map;
        for (size_t i = 0; i < result.num_features(); ++i) {
            result_map[result.name_of(i)] = result.resolve(i).as_number(docid);
        }
        return result_map;
    }
};

TEST_F("require that simple program works", Fixture()) {
    EXPECT_EQUAL(15.0, f1.add("mysum(value(10),ivalue(5))").compile().get());
    EXPECT_EQUAL(3u, f1.program.num_executors());
    EXPECT_EQUAL(3u, count_features(f1.program));
    EXPECT_EQUAL(1u, count_const_features(f1.program));
}

TEST_F("require that const features work", Fixture()) {
    f1.add("mysum(value(10),value(5))").compile();
    EXPECT_EQUAL(15.0, f1.get());
    EXPECT_EQUAL(3u, f1.program.num_executors());
    EXPECT_EQUAL(3u, count_features(f1.program));
    EXPECT_EQUAL(3u, count_const_features(f1.program));
}

TEST_F("require that non-const features work", Fixture()) {
    f1.add("mysum(ivalue(10),ivalue(5))").compile();
    EXPECT_EQUAL(15.0, f1.get());
    EXPECT_EQUAL(3u, f1.program.num_executors());
    EXPECT_EQUAL(3u, count_features(f1.program));
    EXPECT_EQUAL(0u, count_const_features(f1.program));
}

TEST_F("require that a single program can calculate multiple output features", Fixture()) {
    f1.add("value(1)").add("ivalue(2)").add("ivalue(3)");
    f1.add("mysum(value(1),value(2),ivalue(3))");
    f1.compile();
    EXPECT_EQUAL(5u, f1.program.num_executors());
    EXPECT_EQUAL(5u, count_features(f1.program));
    EXPECT_EQUAL(2u, count_const_features(f1.program));
    auto result = f1.all();
    EXPECT_EQUAL(4u, result.size());
    EXPECT_EQUAL(1.0, result["value(1)"]);
    EXPECT_EQUAL(2.0, result["ivalue(2)"]);
    EXPECT_EQUAL(3.0, result["ivalue(3)"]);
    EXPECT_EQUAL(6.0, result["mysum(value(1),value(2),ivalue(3))"]);
}

TEST_F("require that a single executor can produce multiple features", Fixture()) {
    f1.add("mysum(value(1,2,3).0,value(1,2,3).1,value(1,2,3).2)");
    EXPECT_EQUAL(6.0, f1.compile().get());
    EXPECT_EQUAL(2u, f1.program.num_executors());
    EXPECT_EQUAL(4u, count_features(f1.program));
    EXPECT_EQUAL(4u, count_const_features(f1.program));
}

TEST_F("require that feature values can be overridden", Fixture()) {
    f1.add("value(1)").add("ivalue(2)").add("ivalue(3)");
    f1.add("mysum(value(1),value(2),ivalue(3))");
    f1.override("value(2)", 20.0).override("ivalue(3)", 30.0);
    f1.compile();
    EXPECT_EQUAL(5u, f1.program.num_executors());
    EXPECT_EQUAL(5u, count_features(f1.program));
    EXPECT_EQUAL(2u, count_const_features(f1.program));
    auto result = f1.all();
    EXPECT_EQUAL(4u, result.size());
    EXPECT_EQUAL(1.0, result["value(1)"]);
    EXPECT_EQUAL(2.0, result["ivalue(2)"]);
    EXPECT_EQUAL(30.0, result["ivalue(3)"]);
    EXPECT_EQUAL(51.0, result["mysum(value(1),value(2),ivalue(3))"]);    
}

TEST_F("require that the rank program can calculate scores for multiple documents", Fixture()) {
    f1.add("mysum(value(10),docid)").compile();
    EXPECT_EQUAL(3u, count_features(f1.program));
    EXPECT_EQUAL(1u, count_const_features(f1.program));
    EXPECT_EQUAL(f1.get(1), 11.0);
    EXPECT_EQUAL(f1.get(2), 12.0);
    EXPECT_EQUAL(f1.get(3), 13.0);
    EXPECT_EQUAL(f1.get(1), 11.0);
}

TEST_F("require that only non-const features are calculated per document", Fixture()) {
    f1.add("track(mysum(track(value(10)),track(ivalue(5))))").compile();
    EXPECT_EQUAL(6u, f1.program.num_executors());
    EXPECT_EQUAL(6u, count_features(f1.program));
    EXPECT_EQUAL(2u, count_const_features(f1.program));
    EXPECT_EQUAL(f1.track_cnt, 1u);
    EXPECT_EQUAL(15.0, f1.get(1));
    EXPECT_EQUAL(f1.track_cnt, 3u);
    EXPECT_EQUAL(15.0, f1.get(2));
    EXPECT_EQUAL(f1.track_cnt, 5u);
}

TEST_F("require that unused features are not calculated", Fixture()) {
    f1.add("track(ivalue(1))");
    f1.add("track(ivalue(2))");
    f1.compile();
    EXPECT_EQUAL(4u, f1.program.num_executors());
    EXPECT_EQUAL(4u, count_features(f1.program));
    EXPECT_EQUAL(0u, count_const_features(f1.program));
    EXPECT_EQUAL(f1.track_cnt, 0u);
    EXPECT_EQUAL(f1.get("track(ivalue(1))", 1), 1.0);
    EXPECT_EQUAL(f1.track_cnt, 1u);
    EXPECT_EQUAL(f1.get("track(ivalue(2))", 2), 2.0);
    EXPECT_EQUAL(f1.track_cnt, 2u);
    EXPECT_EQUAL(f1.get("track(ivalue(1))", 3), 1.0);
    EXPECT_EQUAL(f1.get("track(ivalue(2))", 3), 2.0);
    EXPECT_EQUAL(f1.track_cnt, 4u);
}

TEST_F("require that re-used features are only calculated once", Fixture()) {
    f1.add("track(mysum(track(ivalue(1)),track(ivalue(1))))").compile();
    EXPECT_EQUAL(4u, f1.program.num_executors());
    EXPECT_EQUAL(4u, count_features(f1.program));
    EXPECT_EQUAL(0u, count_const_features(f1.program));
    EXPECT_EQUAL(f1.track_cnt, 0u);
    EXPECT_EQUAL(f1.get(1), 2.0);
    EXPECT_EQUAL(f1.track_cnt, 2u);
}

TEST_F("require that overrides of const features work for multiple documents", Fixture()) {
    f1.add("mysum(value(1),docid)").override("value(1)", 10.0).compile();
    EXPECT_EQUAL(3u, f1.program.num_executors());
    EXPECT_EQUAL(3u, count_features(f1.program));
    EXPECT_EQUAL(1u, count_const_features(f1.program));
    EXPECT_EQUAL(11.0, f1.get(1));
    EXPECT_EQUAL(12.0, f1.get(2));
    EXPECT_EQUAL(13.0, f1.get(3));
}

TEST_F("require that overrides of non-const features work for multiple documents", Fixture()) {
    f1.add("mysum(docid,ivalue(1))").override("ivalue(1)", 10.0).compile();
    EXPECT_EQUAL(3u, f1.program.num_executors());
    EXPECT_EQUAL(3u, count_features(f1.program));
    EXPECT_EQUAL(0u, count_const_features(f1.program));
    EXPECT_EQUAL(11.0, f1.get(1));
    EXPECT_EQUAL(12.0, f1.get(2));
    EXPECT_EQUAL(13.0, f1.get(3));
}

TEST_F("require that auto-unboxing of const object values work", Fixture()) {
    f1.add("box(value(10))").compile();
    EXPECT_EQUAL(10.0, f1.get());
    EXPECT_EQUAL(2u, f1.program.num_executors());
    EXPECT_EQUAL(3u, count_features(f1.program));
    EXPECT_EQUAL(3u, count_const_features(f1.program));
}

TEST_F("require that auto-unboxing of non-const object values work", Fixture()) {
    f1.add("box(ivalue(10))").compile();
    EXPECT_EQUAL(10.0, f1.get());
    EXPECT_EQUAL(2u, f1.program.num_executors());
    EXPECT_EQUAL(3u, count_features(f1.program));
    EXPECT_EQUAL(0u, count_const_features(f1.program));
}

TEST_MAIN() { TEST_RUN_ALL(); }
