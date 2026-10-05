# Lateral Join Limitations {.dqlh}

An inner-relation can only reference an
outer relation prior to a scope barrier.

**TODO**:  tie this to the connection between
laterals and higher-order. Reference:

### Monads, comprehensions, and the nested relational calculus  {.dqlh}

- E. Moggi (1991). *Notions of computation and monads.* Information and
  Computation 93(1). — Kleisli categories, `bind`/`return` as computation.
- P. Wadler (1992). *Comprehending Monads.* Mathematical Structures in Computer
  Science 2(4). (Also LFP 1990.) — comprehensions = monad `bind`.
- V. Tannen, P. Buneman, L. Wong (1992). *Naturally Embedded Query Languages.*
  ICDT 1992, LNCS 646. — monad-based query languages.
- P. Buneman, S. Naqvi, V. Tannen, L. Wong (1995). *Principles of Programming
  with Complex Objects and Collection Types.* Theoretical Computer Science
  149(1). — **the NRC paper.**
  Calculus.* ACM TODS 25(4). — the monoid comprehension calculus; nesting and
- P. Trinder (1991). *Comprehensions, a query notation for DBPLs.* DBPL 1991.
- T. Grust (2004). *Monad Comprehensions: A Versatile Representation for
  Queries.* In *The Functional Approach to Data Management*, Springer.
- L. Fegaras, D. Maier (2000). *Optimizing Object Queries Using an Effective
  unnesting as calculus rewrites.
- L. Wong (2000). *Kleisli, a Functional Query System.* Journal of Functional
  Programming 10(1). (See also ICFP 2000, "The functional guts of Kleisli.") —
  an engine built on exactly this monad.
  into flat algebra expressions.* ACM TODS 17(1). — flat conservativity, the

### Conservativity / expressive power of nesting {.dqlh}

- J. Paredaens, D. Van Gucht (1992). *Converting nested algebra expressions
  algebra version.
- L. Wong (1996). *Normal Forms and Conservative Extension Properties for Query
  Languages over Collection Types.* Journal of Computer and System Sciences
  52(3). — conservativity for the calculus over all collection types.

### Decorrelation, Apply, and lateral (the engine side) {.dqlh}

- W. Kim (1982). *On optimizing an SQL-like nested query.* ACM TODS 7(3). —
  origin of the **count bug**.
- U. Dayal (1987). *Of Nests and Trees.* VLDB 1987.
- R. Ganski, H. Wong (1987). *Optimization of nested SQL queries revisited.*
  and Aggregation.* SIGMOD 2001. — **Apply removal**; Apply = lateral.
  SIGMOD 1987. — the count-bug fix.
- P. Seshadri, H. Pirahesh, T. Y. C. Leung (1996). *Complex Query
  Decorrelation.* ICDE 1996. — magic decorrelation / driver sets.
- C. Galindo-Legaria, M. Joshi (2001). *Orthogonal Optimization of Subqueries
- M. Elhemali, C. Galindo-Legaria, T. Grabs, M. Joshi (2007). *Execution
  Strategies for SQL Subqueries.* SIGMOD 2007.
- T. Neumann, A. Kemper (2015). *Unnesting Arbitrary Queries.* BTW 2015. —
  any dependent join unnests, given the correlated-attribute domain.

### Modes, adornments, and magic sets (the deductive-DB side) {.dqlh}

- F. Bancilhon, D. Maier, Y. Sagiv, J. Ullman (1986). *Magic Sets and Other
  Strange Ways to Implement Logic Programs.* PODS 1986.
- C. Beeri, R. Ramakrishnan (1991). *On the Power of Magic.* Journal of Logic
  Programming 10. (PODS 1987.)
- J. Ullman (1989). *Principles of Database and Knowledge-Base Systems, Vol.
  II.* Computer Science Press. — adornments, sideways information passing.
- S. Debray, D. S. Warren (1988). *Automatic Mode Inference for Logic
  Programs.* Journal of Logic Programming 5(3). — modes/binding patterns.

### Higher-order logic programming (the axis DelightQL declines) {.dqlh}

- D. H. D. Warren (1982). *Higher-order extensions to Prolog: are they needed?*
  Machine Intelligence 10.
- D. Miller, G. Nadathur (2012). *Programming with Higher-Order Logic.*
  Cambridge University Press. (See also Nadathur & Miller, *An Overview of
  λProlog*, ICLP 1988.) — genuine higher-order logic programming, for
  contrast.

