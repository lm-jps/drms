struct fint_struct {
  int method;
  int order;
  int nsub;
  float fover;
  float *kersx;
  int nshifts;
  int shift0;
  int edgemode;
  float extrapolate;
  char *filename;
};

int init_finterpolate_wiener_old(
  struct fint_struct *pars,
  int order,
  int edgemode, // 0 to go as far as you can with symmetric kernel
                // Otherwise go further (as set by extrapolate)
  float extrapolate, // How far to extrapolate
  int minorder, // Minimum order to use when approaching edge or beyond
  int nconst // Number of polynomial constraints
             // 0 None, 1 for norm, 2 for linear preserved, etc.
);

int init_finterpolate_wiener(
  struct fint_struct *pars,
  int order,
  int edgemode, // 0 to go as far as you can with symmetric kernel
                // Otherwise go further (as set by extrapolate)
  float extrapolate, // How far to extrapolate
  int minorder, // Minimum order to use when approaching edge or beyond
  int nconst, // Number of polynomial constraints
             // 0 None, 1 for norm, 2 for linear preserved, etc.
  int cortable, // Which of the hardcoded tables to use.
                // 0 To use table pointed to by filenamep
  char **filenamep // Pointer to name of file to read covariance from.
                 // Set to actual file used if cortable>0
);

int init_finterpolate_linear(
  struct fint_struct *pars,
  float extrapolate // How far to extrapolate
);

int init_finterpolate_cubic_conv(
  struct fint_struct *pars,
  int edgemode, // 0 to go as far as you can with symmetric kernel
                // Otherwise go further (as set by extrapolate)
  float extrapolate // How far to extrapolate
);

int free_finterpolate(
  struct fint_struct *pars
);

int finterpolate(
  struct fint_struct *pars, // Must have been initialized by init_finterpolate
  float *image_in,
  float *xin,
  float *yin,
  float *image_out,
  int nxin, // Size of input image
  int nyin, // Size of input image
  int nleadin, // Leading dimension of input image. nleadin>=nxin
  int nx, // Size of xin, yin and image_out
  int ny, // Size of xin, yin and image_out
  int nlead, // Leading dimension. nlead>=nx
  float fillval // Value to use if outside area
);

