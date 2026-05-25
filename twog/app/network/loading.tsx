import styles from '../loading-skeleton.module.css';

export default function NetworkLoading() {
  return (
    <div className="site-shell page-shell" aria-busy="true" aria-live="polite">
      <div className={styles.shell}>
        <div className={styles.header}>
          <p className={styles.kicker}>TWOG / Proof Network</p>
          <div
            className={`${styles.skeleton} ${styles.skeletonHeading}`}
            aria-hidden="true"
          />
          <div
            className={`${styles.skeleton} ${styles.skeletonRow} ${styles.medium}`}
            aria-hidden="true"
          />
          <div
            className={`${styles.skeleton} ${styles.skeletonRow} ${styles.short}`}
            aria-hidden="true"
          />
        </div>
        <div className={styles.grid} aria-hidden="true">
          {Array.from({ length: 6 }).map((_, idx) => (
            <div key={idx} className={styles.skeletonCard}>
              <div
                className={`${styles.skeleton} ${styles.skeletonRow} ${styles.short}`}
              />
              <div
                className={`${styles.skeleton} ${styles.skeletonRow} ${styles.medium}`}
              />
              <div className={`${styles.skeleton} ${styles.skeletonRow}`} />
              <div
                className={`${styles.skeleton} ${styles.skeletonRow} ${styles.medium}`}
              />
            </div>
          ))}
        </div>
        <span className={styles.srOnly}>Loading open packets and capsules.</span>
      </div>
    </div>
  );
}
