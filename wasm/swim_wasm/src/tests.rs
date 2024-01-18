// Copyright 2015-2023 Swim Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use wasmtime::{Caller, Config, Engine, FuncType, Linker, Module, Store, Val, ValType};

// https://radu-matei.com/blog/practical-guide-to-wasm-memory/
// https://github.com/andrewdavidmackenzie/wasmi-string/blob/master/main/src/main.rs

#[repr(C)]
struct Gate {
    a: i32,
    b: i32,
}

// #[test]
#[tokio::test]
async fn t() {
    // let runtime = Builder::new_multi_thread().enable_all().build().unwrap();

    let engine = Engine::new(&Config::new().async_support(true)).unwrap();
    let module = Module::from_file(
        &engine,
        "/Users/sircipher/Desktop/work/swim-rust/target/wasm32-unknown-unknown/release/test_wasm.wasm",
    ).unwrap();

    // let wasi = WasiCtxBuilder::new()
    //     .inherit_stdio()
    //     .inherit_stdout()
    //     .inherit_args()
    //     .unwrap()
    //     .build();

    let mut store = Store::new(&engine, ());

    let mut linker = Linker::new(&engine);
    // wasmtime_wasi::add_to_linker(&mut linker, |s| s).unwrap();

    let func = module
        .imports()
        .find(|import| import.name().eq("host_call"))
        .unwrap();

    linker
        .func_new_async(
            func.module(),
            func.name(),
            FuncType::new(vec![ValType::I32], vec![]),
            move |caller: Caller<()>, args: &[Val], ret: &mut [Val]| {
                println!("Guest call: {:?}", args);
                Box::new(async move { Ok(()) })
            },
        )
        .unwrap();

    let mut instance = linker.instantiate_async(&mut store, &module).await.unwrap();
    let func = instance.get_func(&mut store, "init").unwrap();
    let mut agent_ptr = [Val::I32(0)];
    func.call_async(&mut store, &[Val::I32(2)], &mut agent_ptr)
        .await
        .unwrap();

    let func = instance.get_func(&mut store, "dispatch").unwrap();
    func.call_async(&mut store, &agent_ptr, &mut [])
        .await
        .unwrap();
}
