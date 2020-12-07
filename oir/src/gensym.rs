use rand;

pub fn gensym() -> String {
    format!("${:016x}", rand::random::<u64>())
}
