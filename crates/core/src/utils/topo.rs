use crate::utils::graph::Graph;

#[derive(Clone)]
pub struct TopologicalSorter<N: Clone + Eq + Ord> {
    graph: Graph<N, ()>,
}

impl<N> Default for TopologicalSorter<N>
where
    N: Clone + Eq + Ord,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<N: Eq + Ord + Clone> TopologicalSorter<N> {
    pub fn new() -> Self {
        TopologicalSorter { graph: Graph::new() }
    }

    pub fn add_vertex(&mut self, item: N) {
        self.graph.add(item)
    }

    pub fn add_dep(&mut self, from: N, to: N) {
        self.graph.add(from.clone());
        let _ = self.graph.link(to, from.clone());
    }

    pub fn add_deps(&mut self, from: N, tos: impl IntoIterator<Item = N>) {
        for to in tos {
            self.add_dep(from.clone(), to);
        }
    }

    pub fn topological_sort(&self) -> Vec<N> {
        self.graph.sort()
    }

    pub fn dependency_sort(&self) -> Vec<N> {
        let mut result = self.topological_sort();
        result.reverse();
        result
    }
}

#[cfg(test)]
mod graph_tests {
    use super::*;

    #[test]
    fn test_simple_linear_dependency() {
        let mut graph = TopologicalSorter::new();
        graph.add_dep("A", "B");
        graph.add_dep("B", "C");

        let sorted = graph.topological_sort();
        assert_eq!(sorted, vec!["A", "B", "C"]);
    }

    #[test]
    fn test_multiple_sources() {
        let mut graph = TopologicalSorter::new();
        graph.add_dep("A", "C");
        graph.add_dep("B", "C");

        let sorted = graph.topological_sort();
        assert!(sorted == vec!["A", "B", "C"] || sorted == vec!["B", "A", "C"]);
    }

    #[test]
    fn test_complex_dependency() {
        let mut graph = TopologicalSorter::new();
        graph.add_deps("A", ["B", "C"]);
        graph.add_dep("B", "D");
        graph.add_dep("C", "D");
        graph.add_dep("D", "E");

        let sorted = graph.topological_sort();
        assert!(sorted.contains(&"A"));
        assert!(sorted.contains(&"B"));
        assert!(sorted.contains(&"C"));
        assert!(sorted.contains(&"D"));
        assert!(sorted.contains(&"E"));

        let a_index = sorted.iter().position(|&x| x == "A").unwrap();
        let b_index = sorted.iter().position(|&x| x == "B").unwrap();
        let c_index = sorted.iter().position(|&x| x == "C").unwrap();
        let d_index = sorted.iter().position(|&x| x == "D").unwrap();
        let e_index = sorted.iter().position(|&x| x == "E").unwrap();

        assert!(a_index < b_index);
        assert!(a_index < c_index);
        assert!(b_index < d_index);
        assert!(c_index < d_index);
        assert!(d_index < e_index);
    }

    #[test]
    fn test_multiple_layers() {
        let mut graph = TopologicalSorter::new();
        graph.add_dep("A", "C");
        graph.add_dep("B", "C");
        graph.add_dep("C", "D");
        graph.add_dep("D", "E");
        graph.add_dep("E", "F");

        let sorted = graph.topological_sort();
        assert!(sorted == vec!["A", "B", "C", "D", "E", "F"] || sorted == vec!["B", "A", "C", "D", "E", "F"]);
    }

    #[test]
    fn test_cycle_processing() {
        let mut graph = TopologicalSorter::new();
        graph.add_dep("A", "B");
        graph.add_dep("B", "C");
        graph.add_dep("C", "A");

        let sorted = graph.dependency_sort();
        assert!(sorted.len() == 3);
        assert!(sorted.contains(&"A"));
        assert!(sorted.contains(&"B"));
        assert!(sorted.contains(&"C"));
    }

    #[test]
    fn test_multiple_cycles() {
        let mut graph = TopologicalSorter::new();
        graph.add_dep("A", "B");
        graph.add_dep("B", "C");
        graph.add_dep("C", "A");
        graph.add_dep("D", "E");
        graph.add_dep("E", "D");

        let sorted = graph.dependency_sort();
        assert!(sorted.len() == 5);
        assert!(sorted.contains(&"A"));
        assert!(sorted.contains(&"B"));
        assert!(sorted.contains(&"C"));
        assert!(sorted.contains(&"D"));
        assert!(sorted.contains(&"E"));
    }

    #[test]
    fn test_independent_nodes() {
        let mut graph = TopologicalSorter::new();
        graph.add_dep("A", "B");
        graph.add_dep("C", "D");

        let sorted = graph.topological_sort();
        assert!(
            sorted == vec!["A", "B", "C", "D"]
                || sorted == vec!["A", "C", "B", "D"]
                || sorted == vec!["C", "D", "A", "B"]
        );
    }

    #[test]
    fn test_large_graph() {
        let mut graph = TopologicalSorter::new();
        for i in 0..100 {
            for j in (i + 1)..100 {
                graph.add_dep(i.to_string(), j.to_string());
            }
        }

        let sorted = graph.topological_sort();
        for i in 0..100 {
            for j in (i + 1)..100 {
                assert!(
                    sorted.iter().position(|x| x == &i.to_string()) < sorted.iter().position(|x| x == &j.to_string())
                );
            }
        }
    }

    #[test]
    fn test_single_node() {
        let mut graph = TopologicalSorter::new();
        graph.add_dep("A", "A");
        let sorted = graph.topological_sort();

        assert_eq!(sorted, &["A"])
    }

    #[test]
    fn test_no_dependencies() {
        let mut graph = TopologicalSorter::new();
        graph.add_dep("A", "B");
        graph.add_dep("C", "D");
        graph.add_dep("E", "F");

        let sorted = graph.topological_sort();
        assert!(sorted.len() == 6);
        assert!(sorted.contains(&"A"));
        assert!(sorted.contains(&"B"));
        assert!(sorted.contains(&"C"));
        assert!(sorted.contains(&"D"));
        assert!(sorted.contains(&"E"));
        assert!(sorted.contains(&"F"));
    }

    #[test]
    fn test_dependency_sort() {
        let mut graph = TopologicalSorter::new();
        graph.add_deps("A", ["B", "C"]);
        graph.add_dep("B", "D");
        graph.add_dep("C", "D");
        graph.add_dep("D", "E");
        graph.add_vertex("F");

        let sorted = graph.dependency_sort();
        assert_eq!(sorted.len(), 6);
        assert!(sorted.contains(&"A"));
        assert!(sorted.contains(&"B"));
        assert!(sorted.contains(&"C"));
        assert!(sorted.contains(&"D"));
        assert!(sorted.contains(&"E"));
        assert!(sorted.contains(&"F"));

        let a_index = sorted.iter().position(|&x| x == "A").unwrap();
        let b_index = sorted.iter().position(|&x| x == "B").unwrap();
        let c_index = sorted.iter().position(|&x| x == "C").unwrap();
        let d_index = sorted.iter().position(|&x| x == "D").unwrap();
        let e_index = sorted.iter().position(|&x| x == "E").unwrap();
        let f_index = sorted.iter().position(|&x| x == "F").unwrap();

        assert!(f_index == 0 || f_index == 5);
        assert!(b_index < a_index);
        assert!(c_index < a_index);
        assert!(d_index < b_index);
        assert!(d_index < c_index);
        assert!(d_index < a_index);
        assert!(e_index < d_index);
        assert!(e_index < b_index);
        assert!(e_index < c_index);
        assert!(e_index < a_index);
    }
}
