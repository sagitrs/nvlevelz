#ifndef ZIPFIAN_GENERATOR_H
#define ZIPFIAN_GENERATOR_H
#include <math.h>
#include <stdio.h>
#include "util/random.h"
#include <random>
#include <chrono>
//using leveldb::Random;
struct ZipfianGenerator {
    double ZIPFIAN_CONSTANT = 0.99;
    long items_;
    long base_;
    double zipfianconstant_;
    double alpha_, zetan_, eta_, theta_, zeta2theta_;
    long countforzeta_;
    static const bool allowitemcountdecrease = false;

    leveldb::Random uint_rand_;
    std::default_random_engine double_rand_;

    ZipfianGenerator(uint32_t items):
        items_(0),
        base_(0),
        zipfianconstant_(ZIPFIAN_CONSTANT),
        alpha_(0), zetan_(0), eta_(0), theta_(0), zeta2theta_(0),
        countforzeta_(0), uint_rand_(0xdeadbeef),
        double_rand_(static_cast<unsigned>(std::chrono::system_clock::now().time_since_epoch().count()))
    {
        theta_ = zipfianconstant_;
        zeta2theta_ = zeta(2, theta_);
        alpha_ = 1.0 / (1.0 - theta_);

        SetItems(items);
  }
    void SetItems(long items) {
        items_ = items;
        zetan_ = zetastatic(items_, zipfianconstant_);
        countforzeta_ = items_;

        eta_ = (1 - pow(2.0 / items_, 1 - theta_)) / (1 - zeta2theta_ / zetan_);
        nextValue();
    }

  double nextDouble() {
    return std::generate_canonical<double,std::numeric_limits<double>::digits>(double_rand_);
  }
  uint32_t nextLong() {
      return uint_rand_.Next();
  }

  double zeta(long n, double thetaVal) {
    countforzeta_ = n;
    return zetastatic(n, thetaVal);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items,
   * using the zipfian constant theta. This is a static version of the function which will not remember n.
   * @param n The number of items to compute zeta over.
   * @param theta The zipfian constant.
   */
  static double zetastatic(long n, double theta) {
    return zetastatic(0, n, theta, 0);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
   * has n items now but used to have st items. Use the zipfian constant thetaVal. Remember the new value of
   * n so that if we change the itemcount, we'll know to recompute zeta.
   *
   * @param st The number of items used to compute the last initialsum
   * @param n The number of items to compute zeta over.
   * @param thetaVal The zipfian constant.
   * @param initialsum The value of zeta we are computing incrementally from.
   */
  double zeta(long st, long n, double thetaVal, double initialsum) {
    countforzeta_ = n;
    return zetastatic(st, n, thetaVal, initialsum);
  }

  /**
   * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
   * has n items now but used to have st items. Use the zipfian constant theta. Remember the new value of
   * n so that if we change the itemcount, we'll know to recompute zeta.
   * @param st The number of items used to compute the last initialsum
   * @param n The number of items to compute zeta over.
   * @param theta The zipfian constant.
   * @param initialsum The value of zeta we are computing incrementally from.
   */
  static double zetastatic(long st, long n, double theta, double initialsum) {
    double sum = initialsum;
    for (long i = st; i < n; i++) {
        sum += 1 / (pow(i + 1, theta));
    }
    return sum;
  }

  /****************************************************************************************/


  /**
   * Generate the next item as a long.
   *
   * @param itemcount The number of items in the distribution.
   * @return The next item in the sequence.
   */

  long nextZipf(long itemcount) {
    //from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994

    if (itemcount != countforzeta_) {

        if (itemcount > countforzeta_) {
          //System.err.println("WARNING: Incrementally recomputing Zipfian distribtion. (itemcount="+itemcount+"
          // countforzeta="+countforzeta+")");

          //we have added more items. can compute zetan incrementally, which is cheaper
          zetan_ = zeta(countforzeta_, itemcount, theta_, zetan_);
          eta_ = (1 - pow(2.0 / items_, 1 - theta_)) / (1 - zeta2theta_ / zetan_);
        } else if ((itemcount < countforzeta_) && (allowitemcountdecrease)) {
          //have to start over with zetan
          //note : for large itemsets, this is very slow. so don't do it!

          //TODO: can also have a negative incremental computation, e.g. if you decrease the number of items,
          // then just subtract the zeta sequence terms for the items that went away. This would be faster than
          // recomputing from scratch when the number of items decreases

          printf("WARNING: Recomputing Zipfian distribtion. This is slow and should be avoided. ");

          zetan_ = zeta(itemcount, theta_);
          eta_ = (1 - pow(2.0 / items_, 1 - theta_)) / (1 - zeta2theta_ / zetan_);
        }
      }

    double u = nextDouble();
    double uz = u * zetan_;

    if (uz < 1.0) {
      return base_;
    }

    if (uz < 1.0 + pow(0.5, theta_)) {
      return base_ + 1;
    }

    long ret = base_ + (long) ((itemcount) * pow(eta_ * u - eta_ + 1, alpha_));
    //setLastValue(ret);
    return ret;
  }

  long GeneralZipf(long limit, double a = 1.01) {
      double am1, b;

      am1 = a - 1.0;
      b = pow(2.0, am1);

      while (1) {
          double T, U, V, X;

          U = 1.0 - nextDouble();
          V = nextDouble();
          X = floor(pow(U, -1.0/am1));
          /*
           * The real result may be above what can be represented in a signed
           * long. Since this is a straightforward rejection algorithm, we can
           * just reject this value. This function then models a Zipf
           * distribution truncated to sys.maxint.
           */
          if (X > limit || X < 1.0) {
              continue;
          }

          T = pow(1.0 + 1.0/X, am1);
          if (V*X*(T - 1.0)/(b - 1.0) <= T/b) {
              return static_cast<long>(X);
          }
      }
  }
  /**
   * Return the next value, skewed by the Zipfian distribution. The 0th item will be the most popular, followed by
   * the 1st, followed by the 2nd, etc. (Or, if min != 0, the min-th item is the most popular, the min+1th item the
   * next most popular, etc.) If you want the popular items scattered throughout the item space, use
   * ScrambledZipfianGenerator instead.
   */
  long nextValue() {
    return nextZipf(items_);
  }

};

#endif // ZIPFIAN_GENERATOR_H
