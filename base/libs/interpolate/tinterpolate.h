int tinterpolate(
  int nsample, // Number of input times
  double *tsample, // Input times
  double tint, // Target time
  int nconst, // Number of polynomial terms exactly reproduced
  float **images, // Pointer array to input images
  unsigned char **masks, // Pointer array to input masks. 0=good, 1= missing
  float *image_out, // Interpolated image
  int nx, // Number of points in dimension adjacent in memory
  int ny, // Number of points in dimension not adjacent in memory
  int nlead, // Leading dimension of arrays. nlead>=nx
  int method, // Interpolation method
  char **filenamep // Pointer to name of file to read covariance from.
                   // Set to actual file used if method > 0.
);

