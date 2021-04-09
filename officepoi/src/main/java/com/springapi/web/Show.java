package com.springapi.web;

import com.springapi.domain.Product;
import com.springapi.service.ProductService;
import com.springapi.service.impl.ProductServiceImpl;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Show {
    public static void main(String[] args) throws IOException {

        //通过键盘录入Scanner
        Scanner sc = new Scanner(System.in);
        System.out.println("请输入你要选择的功能: 1.导入 2.导出");
        int num = sc.nextInt();
        ProductService productService = new ProductServiceImpl();
        if (num == 1) {
            //1.导入
            //1.1读取excel表中的数据
            System.out.println("请输入您要读取的文件位置(不包含空格)");
            String path = sc.next();
            List<Product> productList = read(path);
            System.out.println(productList);
            //1.2将数据写入到数据库中

            productService.save(productList);
            System.out.println("数据已存入数据库中!");
        } else if (num == 2) {
            //2.导出
            //2.1 读取数据库中的数据
            List<Product> productList = productService.findAll();
            System.out.println(productList);

            //2.2将数据写入到excel表格中
            System.out.println("请输入要写入的文件位置:");
            String path = sc.next();
            write(productList, path);
            System.out.println("写入成功!");

        } else {
            System.out.println("输入有误,请重新启动");
        }
    }

    public static void write(List<Product> productList, String path) throws IOException {
        //1.创建一个工作薄
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook();
        //2.创建工作表
        XSSFSheet sheet = xssfWorkbook.createSheet("商品");
        //单元格样式
        XSSFCellStyle cellStyle = xssfWorkbook.createCellStyle();
        cellStyle.setFillForegroundColor(IndexedColors.PINK.getIndex());
        cellStyle.setFillPattern(CellStyle.SOLID_FOREGROUND);
        //字体样式
        XSSFFont font = xssfWorkbook.createFont();
        font.setFontName("黑体");
        font.setColor(IndexedColors.BLUE.getIndex());
        cellStyle.setFont(font);


        //3.创建行
        XSSFRow row = sheet.createRow(0);
      /* row.createCell(0).setCellValue("商品编号");
       row.createCell(1).setCellValue("商品名称");
       row.createCell(2).setCellValue("商品价格(单位:元/斤)");
       row.createCell(3).setCellValue("商品库存(单位:吨)");*/

        XSSFCell cell = row.createCell(0);
        cell.setCellValue("商品编号");
        cell.setCellStyle(cellStyle);

        XSSFCell cell1 = row.createCell(1);
        cell1.setCellValue("商品名称");
        cell1.setCellStyle(cellStyle);

        XSSFCell cell2 = row.createCell(2);
        cell2.setCellValue("商品价格(单位:元/斤)");
        cell2.setCellStyle(cellStyle);

        XSSFCell cell3 = row.createCell(3);
        cell3.setCellValue("商品库存(单位:吨)");
        cell3.setCellStyle(cellStyle);


        for (int i = 0; i < productList.size(); i++) {
            XSSFRow row1 = sheet.createRow(i + 1);
            row1.createCell(0).setCellValue(productList.get(i).getPid());
            row1.createCell(1).setCellValue(productList.get(i).getPname());
            row1.createCell(2).setCellValue(productList.get(i).getPrice());
            row1.createCell(3).setCellValue(productList.get(i).getPstock());
        }

        FileOutputStream fileOutputStream = new FileOutputStream(path);
        xssfWorkbook.write(fileOutputStream);
        fileOutputStream.flush();
        fileOutputStream.close();
        xssfWorkbook.close();


    }


    public static List<Product> read(String path) throws IOException {

        List<Product> productList = new ArrayList<>();
        //1.获取工作薄
        XSSFWorkbook xssfWorkbook = new XSSFWorkbook(path);
        //2.获取工作表
        XSSFSheet sheet = xssfWorkbook.getSheetAt(0);

        int lastRowNum = sheet.getLastRowNum();
        for (int i = 1; i <= lastRowNum; i++) {
            XSSFRow row = sheet.getRow(i);
            if (row != null) {
                List<String> list = new ArrayList<>();
                for (Cell cell : row) {
                    if (cell != null) {
                        cell.setCellType(Cell.CELL_TYPE_STRING);
                        String value = cell.getStringCellValue();//读取数据
                        if (value != null && !"".equals(value)) {
                            list.add(value);
                        }
                    }
                }
                if (list.size() > 0) {
                    Product product = new Product(Integer.parseInt(list.get(0)), list.get(1), Double.parseDouble(list.get(2)), Integer.parseInt(list.get(3)));
                    productList.add(product);
                }
            }

        }
        return productList;

    }


}
