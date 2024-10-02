use base64::prelude::*;
use color_eyre::eyre::{eyre, Result, Report};
use svg::Document;
use svg::node::element::{Path, Group, Text, Style};
use svg::node::element::path::Data;
use rand::Rng;
use resvg::tiny_skia::Pixmap;
use tiny_skia_path;
use usvg;

/// Roboto provided is provided within the application (vendored).
pub const FONT_FAMILY: &str   = "Roboto";
pub const FONT: &[u8] = include_bytes!("../../../assets/fonts/roboto/Roboto-Regular.ttf");


pub fn plot<P>(prefix: P) -> Result<(), Report>
where
    P: ToString
{
    let prefix = prefix.to_string();

    // ------------------------------------------------------------------------
    // Fonts
 
    log::debug!("Loading fonts.");

    // Convert the vendored TTF fonts to Base64, so we can directly
    // embed the raw font data into the final svg. This ensures a 
    // consistent font experience across platforms.
    let font_base64 = BASE64_STANDARD.encode(FONT);
    let font_css = format!("@font-face {{ font-family: '{FONT_FAMILY}'; src: url('data:application/font-ttf;charset=utf-8;base64,{font_base64}'); }}" );

    // let bold_font_base64 = BASE64_STANDARD.encode(BOLD_FONT);
    // let bold_font_css = format!("@font-face {{ 
    //     font-family: '{bold_font_family}'; src: url('data:application/font-ttf;charset=utf-8;base64,{bold_font_base64}'); }}"
    // );

    let font_size = 30.0;
    let stroke    = 2;

    let mut opt = usvg::Options::default();
    opt.fontdb_mut().load_font_data(FONT.to_vec());
    
    opt.font_family = FONT_FAMILY.to_string();
    opt.font_size   = font_size;

    let top_y  = 0;
    let left_x = 0;    

    // ------------------------------------------------------------------------
    // Parse Data

    log::debug!("Parsing data.");

    let samples = vec![
        "Sample1", 
        "Sample2", 
        "SampleAB", 
        "UnÌcödé Characters",
        "Reeeeeeeeeeeeeeeeeeeeeeeeeeaally Long Label",
    ];

    // stress testing
    // let samples: Vec<_> = (0..100).map(|i| format!("Sample{i}")).collect();

    let mutations = vec![
        "OPG057:H238Q | Tecovirimat",
        "OPG057:A295E | Tecovirimat",
        "OPG071:L108F | Brincidofovir",
        "OPG048:F49F  | Hydroxyurea",
        "OPG180:A50R  | Mitoxantrone",
    ];

    // stress testing
    // let mutations: Vec<_> = (0..100).map(|i| format!("Mutation{i}")).collect();

    // ------------------------------------------------------------------------
    // Text Calculation: Largest Labels

    log::debug!("Calculating largest sample label.");

    // Figure out which the maximum width and height of the sample labels.
    let (sample_width, sample_height) = largest_text(&samples, FONT_FAMILY, font_size, &opt)?;

    // Use the font hide to determine a 'unit' of measurement that will control 
    // the size of the mutation boxes and padding between elements.
    let unit        = if sample_height % 2 == 0 { sample_height } else { sample_height + 1 };
    let padding     = (unit as f32 / 5.0).ceil() as u32;
    let tick_length = unit / 4;

    log::debug!("Calculating largest mutation label.");

    // Figure out which the maximum width and height of the mutation labels.
    let mutation_font_size = font_size;
    // Reversing height and width, because we're going to rotate these labels 90 degrees
    let (mutation_height, _mutation_width) = largest_text(&mutations, FONT_FAMILY, mutation_font_size, &opt)?;

    // ------------------------------------------------------------------------
    // Y Axis: Sample Labels

    log::debug!("Drawing sample labels.");

    let sample_axis_x = left_x + unit + sample_width;
    let sample_axis_y = top_y + unit + mutation_height + padding + tick_length;

    let mut sample_axis = Group::new().set("transform", format!("translate({sample_axis_x} {sample_axis_y})"));

    let x = 0;
    let mut y = (unit / 2) as u32;
    for (i, sample) in samples.iter().enumerate() {
        if i > 0 { y += unit + padding; } 

        // Draw the sample text label
        let sample_text = Text::new(sample.to_string())
            .set("font-size", format!("{font_size}px"))
            .set("font-family", FONT_FAMILY)
            .set("dominant-baseline", "central")
            .set("text-anchor", "end")
            .set("transform", format!("translate({x} {y})"));
        sample_axis = sample_axis.add(sample_text);

        // Draw the horizontal tick that connects to the mutation box row
        let tick_x = x + padding;
        let tick_coords = Data::new().move_to((tick_x, y)).line_by((tick_length, 0));        
        let tick = Path::new().set("stroke", "black").set("stroke-width", stroke).set("d",tick_coords);
        sample_axis = sample_axis.add(tick);       
    }

    // ------------------------------------------------------------------------
    // X axis: Mutation Labels

    log::debug!("Drawing mutation labels.");

    let mutation_axis_x   = sample_axis_x + padding + tick_length;
    let mutation_axis_y   = top_y + unit;
    let mut mutation_axis = Group::new().set("transform", format!("translate({mutation_axis_x} {mutation_axis_y})"));

    // let coord_axis_h = coord_height + padding + tick_length;
    // let coord_axis_w = (coords.len() as u32 * unit ) + ((coords.len() as u32 - 1) * unit);

    let mut x = unit / 2;
    for (i, mutation) in mutations.iter().enumerate() {
        if i > 0 { x += unit + padding; }

        // Draw the mutation Label
        let y = mutation_height;
        let mutation_text = Text::new(mutation.to_string())
            .set("font-size", format!("{mutation_font_size}px"))
            .set("font-family", FONT_FAMILY)
            .set("dominant-baseline", "central")
            .set("text-anchor", "start")
            .set("transform", format!("translate({x} {y}) rotate(-90)"));
        mutation_axis = mutation_axis.add(mutation_text); 

        // Draw the vertical tick that connects to the mutation box column
        let y = mutation_height + padding;
        let tick_coords = Data::new().move_to((x, y)).line_by((0, tick_length));        
        let tick = Path::new().set("stroke", "black").set("stroke-width", stroke).set("d",tick_coords);
        mutation_axis = mutation_axis.add(tick);       
    }

    // ------------------------------------------------------------------------
    // X an Y Axis: Mutation Boxes

    log::debug!("Drawing mutation boxes.");

    let mutation_boxes_x   = mutation_axis_x;
    let mutation_boxes_y   = sample_axis_y;

    let mut mutation_boxes  = Group::new().set("transform", format!("translate({mutation_boxes_x} {mutation_boxes_y})"));
    let mutation_box_coords = Data::new().move_to((0, 0)).line_by((0, unit)).line_by((unit, 0)).line_by((0, -(unit as i32))).close();
    let mutation_box        = Path::new().set("fill", "purple").set("stroke", "black").set("stroke-width", stroke).set("d", mutation_box_coords);

    let mut x = 0;
    // Iterate through mutations ( Moving Left -> Right along the X-Axis)
    for (i, _mutation) in mutations.iter().enumerate() {
        let mut y = 0;
        if i > 0 { x += unit + padding; }
        // Iterate through samples ( Moving Top -> Down along the Y-Axis)
        for (i_s, _) in samples.iter().enumerate() {
            if i_s > 0 { y += unit + padding; }
            // random color 
            let num = rand::thread_rng().gen_range(0..100);
            let fill = match num > 50 {
                true => "purple",
                false => "white",
            };
            let sample_mutation_box = mutation_box
                .clone()
                .set("fill", fill)
                .set("transform", format!("translate({x} {y})") );
            mutation_boxes = mutation_boxes.add(sample_mutation_box);
        }
    }

    let mutation_boxes_w = (mutations.len() as u32 * unit) + ((mutations.len() - 1) as u32 * padding);
    let mutation_boxes_h = (samples.len() as u32 * unit) + ((samples.len() - 1) as u32 * padding);


    // ------------------------------------------------------------------------
    // Render

    log::debug!("Rendering document.");

    let style = Style::new(font_css);

    let document_width = mutation_boxes_x + mutation_boxes_w + unit;
    let document_height = mutation_boxes_y + mutation_boxes_h + unit;

    let background_coords = Data::new().move_to((0, 0)).line_by((0, document_height)).line_by((document_width, 0)).line_by((0, -(document_height as i32))).close();
    let background        = Path::new().set("fill", "white").set("stroke", "white").set("d", background_coords);

    let document = Document::new()
        .set("viewBox", (0, 0, document_width, document_height))
        .add(background)
        .add(style)
        .add(sample_axis)
        .add(mutation_axis)
        .add(mutation_boxes);

    // Render to vector graphics (svg)
    svg::save(format!("{}.svg", prefix), &document)?;
    // Render to pixels (png)
    let tree = usvg::Tree::from_str(&document.to_string(), &opt)?;
    let transform = tiny_skia_path::Transform::default();
    let mut pixmap = Pixmap::new(document_width, document_height).ok_or(eyre!("Failed to create png pixel map: {document_width}x{document_height}"))?;
    resvg::render(&tree, transform, &mut pixmap.as_mut());
    pixmap.save_png(format!("{}.png", prefix))?;


    Ok(())
}

/// Given a list of strings, calculate the maximum width and height needed to accomodate them.
pub fn largest_text<T>(labels: &[T], font_family: &str, font_size: f32, opt: &usvg::Options) -> Result<(u32, u32), Report> 
where
    T: AsRef<str> + std::fmt::Display
{

    let mut max_width: u32 = 0;
    let mut max_height: u32 = 0;

    labels.iter().map(|label| {
        let text_template = format!("<svg xmlns='http://www.w3.org/2000/svg'><text id='text' alignment-baseline='hanging' font-family='{font_family}' font-size='{font_size}px'>");
        let text          = format!("{}{}{}", text_template, label, "</text></svg>");
        let tree          = usvg::Tree::from_str(&text, opt)?;
        let node          = tree.node_by_id("text").ok_or(eyre!("Failed constructing svg node for text: {label}"))?;
        let bounding_box  = node.abs_bounding_box();
        let width         = (bounding_box.right()  - bounding_box.left()).ceil() as u32;
        let height        = (bounding_box.bottom() - bounding_box.top()).ceil() as u32;
        if width  > max_width  { max_width = width }
        if height > max_height { max_height = height }
        Ok(())
    }).collect::<Result<(), Report>>()?;

    Ok((max_width, max_height))
}