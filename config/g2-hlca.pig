img = LOAD '/user/hadoop/img_parsed.txt' USING PigStorage(' ') as (block, band, pixel,value:float);
group_img = GROUP img BY (block, band);
centroids = FOREACH group_img GENERATE group.block, group.band, (SUM(img.value)/1024) as centroid;
STORE centroids INTO '/user/hadoop/centroids' USING PigStorage(',');

block_centroid = JOIN img BY (block,band), centroids BY (block,band);
centralized = FOREACH block_centroid GENERATE img::block as block, img::band as band, img::pixel as pixel, img::value - centroids::centroid as centralized;

/* Iteracion 1 */
pow_1 = FOREACH centralized  GENERATE block as block, pixel as pixel, band as band, (centralized * centralized) as centralized_pow;
group_brillo_1 = GROUP pow_1 BY (block,pixel);
b_1 = FOREACH group_brillo_1 GENERATE group.block as block, group.pixel as pixel, SUM(pow_1.centralized_pow) as pixel_brightness;

b_group_1 = GROUP b_1 BY block; 
b_max_1 = FOREACH b_group_1 {
    ordered = ORDER b_1 BY pixel_brightness DESC;
    max_bright = limit ordered 1;
    generate flatten(max_bright);
}

b_max_join_1 = JOIN centralized by (block, pixel), b_max_1 by (max_bright::block, max_bright::pixel);
q_1 = FOREACH b_max_join_1 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized as centralized;
STORE q_1 INTO '/user/hadoo/q_1' USING PigStorage(',');

u_1 = FOREACH b_max_join_1 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized / b_max_1::max_bright::pixel_brightness as value;

v_join_1 = JOIN centralized BY (block, band), u_1 BY (block, band);
v_product_1 = FOREACH v_join_1 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, (centralized::centralized * u_1::value) as value;
v_product_group_1 = GROUP v_product_1 BY (block, pixel);
v_1 = FOREACH v_product_group_1 GENERATE group.block, group.pixel as pixel, SUM(v_product_1.value) as value;
STORE v_1 INTO '/user/hadoop/v_1' USING PigStorage(',');

x_join_1 = JOIN q_1 by block, v_1 by block;
new_x_join_1 = FOREACH x_join_1 GENERATE q_1::block as block_q, q_1::band as band, q_1::centralized as centralized, v_1::block as block_v, v_1::pixel as pixel, v_1::value as value;
x_product_1 = FOREACH new_x_join_1 GENERATE block_q as block, pixel, (centralized) * (value) as value;
x_group_product_1 = GROUP x_product_1 by (block, pixel);
x_scalar_1 = FOREACH x_group_product_1 GENERATE group.block as block, group.pixel as pixel, SUM(x_product_1.value) as value;
x_join_scalar_1 = JOIN centralized by (block, pixel), x_scalar_1 by (block, pixel);
centralized = FOREACH x_join_scalar_1 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized - x_scalar_1::value as centralized;

/* Iteracion 2 */
pow_2 = FOREACH centralized  GENERATE block as block, pixel as pixel, band as band, (centralized * centralized) as centralized_pow;
group_brillo_2 = GROUP pow_2 BY (block,pixel);
b_2 = FOREACH group_brillo_2 GENERATE group.block as block, group.pixel as pixel, SUM(pow_2.centralized_pow) as pixel_brightness;

b_group_2 = GROUP b_2 BY block; 
b_max_2 = FOREACH b_group_2 {
    ordered = ORDER b_2 BY pixel_brightness DESC;
    max_bright = limit ordered 1;
    generate flatten(max_bright);
}

b_max_join_2 = JOIN centralized by (block, pixel), b_max_2 by (max_bright::block, max_bright::pixel);
q_2 = FOREACH b_max_join_2 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized as centralized;
STORE q_2 INTO '/user/hadoo/q_2' USING PigStorage(',');

u_2 = FOREACH b_max_join_2 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized / b_max_2::max_bright::pixel_brightness as value;

v_join_2 = JOIN centralized BY (block, band), u_2 BY (block, band);
v_product_2 = FOREACH v_join_2 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, (centralized::centralized * u_2::value) as value;
v_product_group_2 = GROUP v_product_2 BY (block, pixel);
v_2 = FOREACH v_product_group_2 GENERATE group.block, group.pixel as pixel, SUM(v_product_2.value) as value;
STORE v_2 INTO '/user/hadoop/v_2' USING PigStorage(',');

x_join_2 = JOIN q_2 by block, v_2 by block;
new_x_join_2 = FOREACH x_join_2 GENERATE q_2::block as block_q, q_2::band as band, q_2::centralized as centralized, v_2::block as block_v, v_2::pixel as pixel, v_2::value as value;
x_product_2 = FOREACH new_x_join_2 GENERATE block_q as block, pixel, (centralized) * (value) as value;
x_group_product_2 = GROUP x_product_2 by (block, pixel);
x_scalar_2 = FOREACH x_group_product_2 GENERATE group.block as block, group.pixel as pixel, SUM(x_product_2.value) as value;
x_join_scalar_2 = JOIN centralized by (block, pixel), x_scalar_2 by (block, pixel);
centralized = FOREACH x_join_scalar_2 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized - x_scalar_2::value as centralized;

/* Iteracion 3 */
pow_3 = FOREACH centralized  GENERATE block as block, pixel as pixel, band as band, (centralized * centralized) as centralized_pow;
group_brillo_3 = GROUP pow_3 BY (block,pixel);
b_3 = FOREACH group_brillo_3 GENERATE group.block as block, group.pixel as pixel, SUM(pow_3.centralized_pow) as pixel_brightness;

b_group_3 = GROUP b_3 BY block; 
b_max_3 = FOREACH b_group_3 {
    ordered = ORDER b_3 BY pixel_brightness DESC;
    max_bright = limit ordered 1;
    generate flatten(max_bright);
}

b_max_join_3 = JOIN centralized by (block, pixel), b_max_3 by (max_bright::block, max_bright::pixel);
q_3 = FOREACH b_max_join_3 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized as centralized;
STORE q_3 INTO '/user/hadoo/q_3' USING PigStorage(',');

u_3 = FOREACH b_max_join_3 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized / b_max_3::max_bright::pixel_brightness as value;

v_join_3 = JOIN centralized BY (block, band), u_3 BY (block, band);
v_product_3= FOREACH v_join_3 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, (centralized::centralized * u_3::value) as value;
v_product_group_3 = GROUP v_product_3 BY (block, pixel);
v_3 = FOREACH v_product_group_3 GENERATE group.block, group.pixel as pixel, SUM(v_product_3.value) as value;
STORE v_3 INTO '/user/hadoop/v_3' USING PigStorage(',');

x_join_3 = JOIN q_3 by block, v_3 by block;
new_x_join_3 = FOREACH x_join_3 GENERATE q_3::block as block_q, q_3::band as band, q_3::centralized as centralized, v_3::block as block_v, v_3::pixel as pixel, v_3::value as value;
x_product_3 = FOREACH new_x_join_3 GENERATE block_q as block, pixel, (centralized) * (value) as value;
x_group_product_3 = GROUP x_product_3 by (block, pixel);
x_scalar_3 = FOREACH x_group_product_3 GENERATE group.block as block, group.pixel as pixel, SUM(x_product_3.value) as value;
x_join_scalar_3 = JOIN centralized by (block, pixel), x_scalar_3 by (block, pixel);
centralized = FOREACH x_join_scalar_3 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized - x_scalar_3::value as centralized;

/* Iteracion 4 */
pow_4 = FOREACH centralized  GENERATE block as block, pixel as pixel, band as band, (centralized * centralized) as centralized_pow;
group_brillo_4 = GROUP pow_4 BY (block,pixel);
b_4 = FOREACH group_brillo_4 GENERATE group.block as block, group.pixel as pixel, SUM(pow_4.centralized_pow) as pixel_brightness;

b_group_4 = GROUP b_4 BY block; 
b_max_4 = FOREACH b_group_4 {
    ordered = ORDER b_4 BY pixel_brightness DESC;
    max_bright = limit ordered 1;
    generate flatten(max_bright);
}

b_max_join_4 = JOIN centralized by (block, pixel), b_max_4 by (max_bright::block, max_bright::pixel);
q_4 = FOREACH b_max_join_4 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized as centralized;
STORE q_4 INTO '/user/hadoo/q_4' USING PigStorage(',');

u_4 = FOREACH b_max_join_4 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized / b_max_4::max_bright::pixel_brightness as value;

v_join_4 = JOIN centralized BY (block, band), u_4 BY (block, band);
v_product_4 = FOREACH v_join_4 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, (centralized::centralized * u_4::value) as value;
v_product_group_4 = GROUP v_product_4 BY (block, pixel);
v_4 = FOREACH v_product_group_4 GENERATE group.block, group.pixel as pixel, SUM(v_product_4.value) as value;
STORE v_4 INTO '/user/hadoop/v_4' USING PigStorage(',');

x_join_4 = JOIN q_4 by block, v_4 by block;
new_x_join_4 = FOREACH x_join_4 GENERATE q_4::block as block_q, q_4::band as band, q_4::centralized as centralized, v_4::block as block_v, v_4::pixel as pixel, v_4::value as value;
x_product_4 = FOREACH new_x_join_4 GENERATE block_q as block, pixel, (centralized) * (value) as value;
x_group_product_4 = GROUP x_product_4 by (block, pixel);
x_scalar_4 = FOREACH x_group_product_4 GENERATE group.block as block, group.pixel as pixel, SUM(x_product_4.value) as value;
x_join_scalar_4 = JOIN centralized by (block, pixel), x_scalar_4 by (block, pixel);
centralized = FOREACH x_join_scalar_4 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized - x_scalar_4::value as centralized;

/* Iteracion 5 */
pow_5 = FOREACH centralized  GENERATE block as block, pixel as pixel, band as band, (centralized * centralized) as centralized_pow;
group_brillo_5 = GROUP pow_5 BY (block,pixel);
b_5 = FOREACH group_brillo_5 GENERATE group.block as block, group.pixel as pixel, SUM(pow_5.centralized_pow) as pixel_brightness;

b_group_5 = GROUP b_5 BY block; 
b_max_5 = FOREACH b_group_5 {
    ordered = ORDER b_5 BY pixel_brightness DESC;
    max_bright = limit ordered 1;
    generate flatten(max_bright);
}

b_max_join_5 = JOIN centralized by (block, pixel), b_max_5 by (max_bright::block, max_bright::pixel);
q_5 = FOREACH b_max_join_5 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized as centralized;
STORE q_5 INTO '/user/hadoo/q_5' USING PigStorage(',');

u_5 = FOREACH b_max_join_5 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized / b_max_5::max_bright::pixel_brightness as value;

v_join_5 = JOIN centralized BY (block, band), u_5 BY (block, band);
v_product_5 = FOREACH v_join_5 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, (centralized::centralized * u_5::value) as value;
v_product_group_5 = GROUP v_product_5 BY (block, pixel);
v_5 = FOREACH v_product_group_5 GENERATE group.block, group.pixel as pixel, SUM(v_product_5.value) as value;
STORE v_5 INTO '/user/hadoop/v_5' USING PigStorage(',');

x_join_5 = JOIN q_5 by block, v_5 by block;
new_x_join_5 = FOREACH x_join_5 GENERATE q_5::block as block_q, q_5::band as band, q_5::centralized as centralized, v_5::block as block_v, v_5::pixel as pixel, v_5::value as value;
x_product_5 = FOREACH new_x_join_5 GENERATE block_q as block, pixel, (centralized) * (value) as value;
x_group_product_5 = GROUP x_product_5 by (block, pixel);
x_scalar_5 = FOREACH x_group_product_5 GENERATE group.block as block, group.pixel as pixel, SUM(x_product_5.value) as value;
x_join_scalar_5 = JOIN centralized by (block, pixel), x_scalar_5 by (block, pixel);
centralized = FOREACH x_join_scalar_5 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized - x_scalar_5::value as centralized;

/* Iteracion 6 */
pow_6 = FOREACH centralized  GENERATE block as block, pixel as pixel, band as band, (centralized * centralized) as centralized_pow;
group_brillo_6 = GROUP pow_6 BY (block,pixel);
b_6 = FOREACH group_brillo_6 GENERATE group.block as block, group.pixel as pixel, SUM(pow_6.centralized_pow) as pixel_brightness;

b_group_6 = GROUP b_6 BY block; 
b_max_6 = FOREACH b_group_6 {
    ordered = ORDER b_6 BY pixel_brightness DESC;
    max_bright = limit ordered 1;
    generate flatten(max_bright);
}

b_max_join_6 = JOIN centralized by (block, pixel), b_max_6 by (max_bright::block, max_bright::pixel);
q_6 = FOREACH b_max_join_6 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized as centralized;
STORE q_6 INTO '/user/hadoo/q_6' USING PigStorage(',');

u_6 = FOREACH b_max_join_6 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized / b_max_6::max_bright::pixel_brightness as value;

v_join_6 = JOIN centralized BY (block, band), u_6 BY (block, band);
v_product_6 = FOREACH v_join_6 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, (centralized::centralized * u_6::value) as value;
v_product_group_6 = GROUP v_product_6 BY (block, pixel);
v_6 = FOREACH v_product_group_6 GENERATE group.block, group.pixel as pixel, SUM(v_product_6.value) as value;
STORE v_6 INTO '/user/hadoop/v_6' USING PigStorage(',');

x_join_6 = JOIN q_6 by block, v_6 by block;
new_x_join_6 = FOREACH x_join_6 GENERATE q_6::block as block_q, q_6::band as band, q_6::centralized as centralized, v_6::block as block_v, v_6::pixel as pixel, v_6::value as value;
x_product_6 = FOREACH new_x_join_6 GENERATE block_q as block, pixel, (centralized) * (value) as value;
x_group_product_6 = GROUP x_product_6 by (block, pixel);
x_scalar_6 = FOREACH x_group_product_6 GENERATE group.block as block, group.pixel as pixel, SUM(x_product_6.value) as value;
x_join_scalar_6 = JOIN centralized by (block, pixel), x_scalar_6 by (block, pixel);
centralized = FOREACH x_join_scalar_6 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized - x_scalar_6::value as centralized;

/* Iteracion 7 */
pow_7  = FOREACH centralized  GENERATE block as block, pixel as pixel, band as band, (centralized * centralized) as centralized_pow;
group_brillo_7 = GROUP pow_7 BY (block,pixel);
b_7 = FOREACH group_brillo_7 GENERATE group.block as block, group.pixel as pixel, SUM(pow_7.centralized_pow) as pixel_brightness;

b_group_7 = GROUP b_7 BY block; 
b_max_7 = FOREACH b_group_7 {
    ordered = ORDER b_7 BY pixel_brightness DESC;
    max_bright = limit ordered 1;
    generate flatten(max_bright);
}

b_max_join_7 = JOIN centralized by (block, pixel), b_max_7 by (max_bright::block, max_bright::pixel);
q_7 = FOREACH b_max_join_7 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized as centralized;
STORE q_7 INTO '/user/hadoo/q_7' USING PigStorage(',');

u_7 = FOREACH b_max_join_7 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized / b_max_7::max_bright::pixel_brightness as value;

v_join_7 = JOIN centralized BY (block, band), u_7 BY (block, band);
v_product_7 = FOREACH v_join_7 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, (centralized::centralized * u_7::value) as value;
v_product_group_7 = GROUP v_product_7 BY (block, pixel);
v_7 = FOREACH v_product_group_7 GENERATE group.block, group.pixel as pixel, SUM(v_product_7.value) as value;
STORE v_7 INTO '/user/hadoop/v_7' USING PigStorage(',');

x_join_7 = JOIN q_7 by block, v_7 by block;
new_x_join_7 = FOREACH x_join_7 GENERATE q_7::block as block_q, q_7::band as band, q_7::centralized as centralized, v_7::block as block_v, v_7::pixel as pixel, v_7::value as value;
x_product_7 = FOREACH new_x_join_7 GENERATE block_q as block, pixel, (centralized) * (value) as value;
x_group_product_7 = GROUP x_product_7 by (block, pixel);
x_scalar_7 = FOREACH x_group_product_7 GENERATE group.block as block, group.pixel as pixel, SUM(x_product_7.value) as value;
x_join_scalar_7 = JOIN centralized by (block, pixel), x_scalar_7 by (block, pixel);
centralized = FOREACH x_join_scalar_7 GENERATE centralized::block as block, centralized::band as band, centralized::pixel as pixel, centralized::centralized - x_scalar_7::value as centralized;
